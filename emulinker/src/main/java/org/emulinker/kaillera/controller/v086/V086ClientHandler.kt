package org.emulinker.kaillera.controller.v086

import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Timer
import com.google.common.flogger.FluentLogger
import io.netty.buffer.ByteBuf
import io.netty.channel.socket.DatagramPacket
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds
import org.emulinker.config.RuntimeFlags
import org.emulinker.kaillera.controller.CombinedKailleraController
import org.emulinker.kaillera.controller.messaging.MessageFormatException
import org.emulinker.kaillera.controller.messaging.ParseException
import org.emulinker.kaillera.controller.v086.action.CachedGameDataAction
import org.emulinker.kaillera.controller.v086.action.FatalActionException
import org.emulinker.kaillera.controller.v086.action.GameDataAction
import org.emulinker.kaillera.controller.v086.action.V086Action
import org.emulinker.kaillera.controller.v086.action.V086GameEventHandler
import org.emulinker.kaillera.controller.v086.action.V086ServerEventHandler
import org.emulinker.kaillera.controller.v086.action.V086UserEventHandler
import org.emulinker.kaillera.controller.v086.protocol.CachedGameData
import org.emulinker.kaillera.controller.v086.protocol.GameData
import org.emulinker.kaillera.controller.v086.protocol.V086Bundle
import org.emulinker.kaillera.controller.v086.protocol.V086BundleFormatException
import org.emulinker.kaillera.controller.v086.protocol.V086Message
import org.emulinker.kaillera.model.KailleraUser
import org.emulinker.kaillera.model.event.GameDataEvent
import org.emulinker.kaillera.model.event.GameEvent
import org.emulinker.kaillera.model.event.KailleraEvent
import org.emulinker.kaillera.model.event.ServerEvent
import org.emulinker.kaillera.model.event.UserEvent
import org.emulinker.util.EmuUtil
import org.emulinker.util.EmuUtil.dumpToByteArray
import org.emulinker.util.EmuUtil.timeKt
import org.emulinker.util.FastGameDataCache
import org.emulinker.util.GameDataCache
import org.emulinker.util.stripFromProdBinary
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject

class V086ClientHandler(
  // TODO(nue): Try to replace this with remoteSocketAddress.
  /** I think this is the address from when the user called the connect controller. */
  val connectRemoteSocketAddress: InetSocketAddress,
  val controller: V086Controller,
  /** The [CombinedKailleraController] that created this instance. */
  private val combinedKailleraController: CombinedKailleraController,
) : KoinComponent {
  private val metrics: MetricRegistry by inject()
  private val flags: RuntimeFlags by inject()

  /** Mutex ensuring that only one packet is processed at a time for this [V086ClientHandler]. */
  private val sendMutex = Object()

  var remoteSocketAddress: InetSocketAddress? = null
    private set

  fun handleReceived(buf: ByteBuf, remoteSocketAddress: InetSocketAddress) {
    if (this.remoteSocketAddress == null) {
      this.remoteSocketAddress = remoteSocketAddress
    } else if (remoteSocketAddress != this.remoteSocketAddress) {
      logger
        .atSevere()
        .log(
          "Rejecting packet received from wrong address: %s != %s. This should not be possible!",
          EmuUtil.formatSocketAddress(remoteSocketAddress),
          EmuUtil.formatSocketAddress(this.remoteSocketAddress!!),
        )
      return
    }
    if (clientRequestTimer != null) {
      clientRequestTimer.timeKt { handleReceivedInternal(buf) }
    } else {
      handleReceivedInternal(buf)
    }
  }

  lateinit var user: KailleraUser
    private set

  // TODO(nue): Add this to RuntimeFlags and increase to at least 5.
  val numAcksForSpeedTest = 3

  private var prevMessageNumber = -1

  private var lastMessageNumber = -1

  val clientGameDataCache: GameDataCache = FastGameDataCache(256)

  val serverGameDataCache: GameDataCache = FastGameDataCache(256)

  // Holds more than a bundle needs (MAX_BUNDLE_SIZE) so resendFromSerial() can go back ~1 s of a
  // game; normal sends and resend() still only take the newest few.
  private val lastMessageBuffer = LastMessageBuffer(V086Controller.SERIAL_RESEND_BUFFER_SIZE)
  private val outMessages = arrayOfNulls<V086Message>(V086Controller.MAX_BUNDLE_SIZE)
  private val serialResendMessages = arrayOfNulls<V086Message>(V086Controller.SERIAL_RESEND_BUFFER_SIZE)
  private var lastSerialResend = 0L
  private var testStart: Long = 0
  private var lastMeasurement: Long = 0
  var speedMeasurementCount = 0
    private set

  var bestNetworkSpeed = Int.MAX_VALUE
    private set

  private var clientRetryCount = 0
  private var lastResend = 0L

  private val clientRequestTimer: Timer? =
    if (flags.metricsEnabled) {
      metrics.timer(MetricRegistry.name(this.javaClass, "InboundRequests"))
    } else {
      null
    }

  private val clientResponseMeter: Meter? =
    if (flags.metricsEnabled) {
      metrics.meter(MetricRegistry.name(this.javaClass, "OutboundRequests"))
    } else {
      null
    }

  private val lastSendMessageNumber = AtomicInteger(0)

  private fun getAndIncrementSendMessageNumber(): Int = lastSendMessageNumber.getAndUpdate {
    (if (it > 0xFFFF) 0 else it) + 1
  }

  fun resetGameDataCache() {
    clientGameDataCache.clear()
    serverGameDataCache.clear()
  }

  fun startSpeedTest() {
    lastMeasurement = System.nanoTime()
    testStart = lastMeasurement
    speedMeasurementCount = 0
  }

  fun addSpeedMeasurement() {
    val et = (System.nanoTime() - lastMeasurement).toInt()
    if (et < bestNetworkSpeed) {
      bestNetworkSpeed = et
    }
    speedMeasurementCount++
    lastMeasurement = System.nanoTime()
  }

  val averageNetworkSpeed: Duration
    get() = (lastMeasurement - testStart).nanoseconds / speedMeasurementCount

  fun start(user: KailleraUser) {
    this.user = user
    controller.clientHandlers[user.id] = this
  }

  fun stop() {
    controller.clientHandlers.remove(user.id)
    combinedKailleraController.clientHandlers.remove(remoteSocketAddress)
    synchronized(sendMutex) { lastMessageBuffer.releaseAll() }
    resetGameDataCache()
  }

  override fun toString(): String = "[V086ClientHandler $user]"

  // TODO(nue): This probably needs to be synchronized because of the last read number.
  private fun handleReceivedInternal(buffer: ByteBuf) {
    // Serial resend request (n02 TIERES DLL): [0x00][first missing message number, LE16]. During
    // a game the client lost server messages its bundles can't bring back any more - a lost
    // GAMEDATA/GAMCDATA would otherwise shift its input for the rest of the match (a desync).
    // A normal bundle always holds 1..32 messages, so a leading 0 can't be one.
    if (buffer.readableBytes() == 3 && buffer.getByte(buffer.readerIndex()).toInt() == 0) {
      resendFromSerial(buffer.getUnsignedShortLE(buffer.readerIndex() + 1))
      return
    }
    val inBundle: V086Bundle =
      try {
        V086Bundle.parse(buffer, lastMessageNumber)
      } catch (e: ParseException) {
        buffer.resetReaderIndex()
        logger
          .atWarning()
          .withCause(e)
          .log("%s failed to parse: %s", this, buffer.dumpToByteArray().toHexString())
        null
      } catch (e: V086BundleFormatException) {
        buffer.resetReaderIndex()
        logger
          .atWarning()
          .withCause(e)
          .log(
            "%s received invalid message bundle: %s",
            this,
            buffer.dumpToByteArray().toHexString(),
          )
        null
      } catch (e: MessageFormatException) {
        logger
          .atWarning()
          .withCause(e)
          .log("%s received invalid message: %s}", this, buffer.dumpToByteArray().toHexString())
        null
      } ?: return

    stripFromProdBinary {
      logger
        .atFinest()
        .log(
          "<- FROM P%d: %s",
          user.id,
          when (inBundle) {
            is V086Bundle.Single -> inBundle.message
            is V086Bundle.Multi -> inBundle.messages.firstOrNull()
          },
        )
    }
    clientRetryCount =
      if (inBundle is V086Bundle.Multi && inBundle.numMessages == 0) {
        logger
          .atFine()
          .atMostEvery(1, TimeUnit.SECONDS)
          .log("%s received bundle of messages from %s", this, user)
        clientRetryCount++
        resend(clientRetryCount)
        return
      } else {
        0
      }

    try {
      when (inBundle) {
        is V086Bundle.Single -> {
          val m = inBundle.message
          lastMessageNumber = m.messageNumber
          val action: V086Action<out V086Message>? =
            // Checking for GameData first is a speed optimization.
            when (m.messageTypeId) {
              GameData.ID -> GameDataAction
              CachedGameData.ID -> CachedGameDataAction
              else -> controller.actions[m.messageTypeId.toInt()]
            }
          if (action == null) {
            logger
              .atSevere()
              .log("No action defined to handle client message: %s", inBundle.message)
            return
          }
          (action as V086Action<V086Message>).performAction(m, this)
        }

        is V086Bundle.Multi -> {
          val messages = inBundle.messages
          // read the bundle from back to front to process the oldest messages first
          for (i in inBundle.numMessages - 1 downTo 0) {
            /**
             * already extracts messages with higher numbers when parsing, it does not need to be
             * checked and this causes an error if messageNumber is 0 and lastMessageNumber is
             * 0xFFFF if (messages [i].getNumber() > lastMessageNumber)
             */
            prevMessageNumber = lastMessageNumber
            val m: V086Message = messages[i]!!
            lastMessageNumber = m.messageNumber
            if (prevMessageNumber + 1 != lastMessageNumber) {
              if (prevMessageNumber == 0xFFFF && lastMessageNumber == 0) {
                // exception; do nothing
              } else {
                logger
                  .atWarning()
                  .log(
                    "%s dropped a packet! (%d to %d)",
                    user,
                    prevMessageNumber,
                    lastMessageNumber,
                  )
                user.droppedPacket()
              }
            }
            val action: V086Action<out V086Message>? =
              // Checking for GameData first is a speed optimization.
              when (m.messageTypeId) {
                GameData.ID -> GameDataAction
                CachedGameData.ID -> CachedGameDataAction
                else -> controller.actions[m.messageTypeId.toInt()]
              }
            if (action == null) {
              logger.atSevere().log("No action defined to handle client message: %s", messages[i])
            } else {
              // logger.atFine().log(user + " -> " + message);
              (action as V086Action<V086Message>).performAction(m, this)
            }
          }
        }
      }
    } catch (e: FatalActionException) {
      logger.atWarning().withCause(e).log("%s fatal action, closing connection", this)
      stop()
    } finally {
      // Release any GameData messages that were in the bundle.
      inBundle.release()
    }
  }

  fun actionPerformed(event: KailleraEvent) {
    when (event) {
      // Check for GameDataEvent first to avoid map lookup slowness.
      is GameDataEvent -> {
        GameDataAction.handleEvent(event, this)
      }

      is GameEvent -> {
        val eventHandler = controller.gameEventHandlers[event::class]
        if (eventHandler == null) {
          logger
            .atSevere()
            .log("%s found no GameEventHandler registered to handle game event: %s", this, event)
          return
        }
        (eventHandler as V086GameEventHandler<GameEvent>).handleEvent(event, this)
      }

      is ServerEvent -> {
        val eventHandler = controller.serverEventHandlers[event::class]
        if (eventHandler == null) {
          logger
            .atSevere()
            .log(
              "%s found no ServerEventHandler registered to handle server event: %s",
              this,
              event,
            )
          return
        }
        (eventHandler as V086ServerEventHandler<ServerEvent>).handleEvent(event, this)
      }

      is UserEvent -> {
        val eventHandler = controller.userEventHandlers[event::class]
        if (eventHandler == null) {
          logger
            .atSevere()
            .log("%s found no UserEventHandler registered to handle user event: ", this, event)
          return
        }
        (eventHandler as V086UserEventHandler<UserEvent>).handleEvent(event, this)
      }
    }
  }

  // Caching here for speed.
  private val maxPingMs: Long = flags.maxPing.inWholeMilliseconds

  fun resend(timeoutCounter: Int) {
    // if ((System.currentTimeMillis() - lastResend) > (user.getPing()*3))
    if (System.currentTimeMillis() - lastResend > maxPingMs) {
      // int numToSend = (3+timeoutCounter);
      var numToSend = 3 * timeoutCounter
      if (numToSend > V086Controller.MAX_BUNDLE_SIZE) numToSend = V086Controller.MAX_BUNDLE_SIZE

      stripFromProdBinary {
        logger.atFinest().log("%s: resending last %d messages", this, numToSend)
      }
      resendFromCache(numToSend)
      lastResend = System.currentTimeMillis()
    } else {
      stripFromProdBinary { logger.atFinest().log("Skipping resend...") }
    }
  }

  private fun resendFromCache(numToSend: Int = 5) {
    synchronized(sendMutex) {
      var numToSend = numToSend

      val buf = combinedKailleraController.alloc().directBuffer(flags.v086BufferSize)
      try {
        numToSend = lastMessageBuffer.fill(outMessages, numToSend)
        val outBundle = V086Bundle.Multi(outMessages, numToSend)
        stripFromProdBinary { logger.atFinest().log("<- TO P%d: (RESEND)", user.id) }
        outBundle.writeTo(buf)
        combinedKailleraController.send(DatagramPacket(buf, remoteSocketAddress!!))
        clientResponseMeter?.mark()
      } catch (e: Throwable) {
        buf.release()
        throw e
      }
    }
  }

  /**
   * Resends every message from [serial] (the client's first missing one) up to the newest, oldest
   * of them still in [lastMessageBuffer]. Bundles go out newest message first, as usual; the
   * client fills its holes from any of them and ignores what it already has.
   */
  fun resendFromSerial(serial: Int) {
    synchronized(sendMutex) {
      val now = System.currentTimeMillis()
      if (now - lastSerialResend < V086Controller.SERIAL_RESEND_MIN_INTERVAL_MS) return
      lastSerialResend = now

      val n = lastMessageBuffer.fill(serialResendMessages, V086Controller.SERIAL_RESEND_BUFFER_SIZE)
      val newest = serialResendMessages.firstOrNull()?.messageNumber ?: return
      val span = (newest - serial) and 0xFFFF
      if (span >= 0x8000) return // the client asks for something newer than anything sent
      var count = 0
      while (count < n) {
        val m = serialResendMessages[count] ?: break
        if (((m.messageNumber - serial) and 0xFFFF) > span) break // older than requested
        count++
      }
      if (count == 0) return
      stripFromProdBinary {
        logger.atFinest().log("<- TO P%d: (RESEND %d messages from #%d)", user.id, count, serial)
      }

      var first = 0
      while (first < count) {
        var end = first
        var bytes = 1
        while (end < count && end - first < V086Controller.MAX_RESEND_BUNDLE_MESSAGES) {
          val size = 4 + serialResendMessages[end]!!.bodyBytesPlusMessageIdType
          if (end > first && bytes + size > flags.v086BufferSize) break
          bytes += size
          end++
        }
        val chunk = arrayOfNulls<V086Message>(end - first)
        System.arraycopy(serialResendMessages, first, chunk, 0, end - first)
        val buf = combinedKailleraController.alloc().directBuffer(flags.v086BufferSize)
        try {
          V086Bundle.Multi(chunk, chunk.size).writeTo(buf)
          combinedKailleraController.send(DatagramPacket(buf, remoteSocketAddress!!))
          clientResponseMeter?.mark()
        } catch (e: Throwable) {
          buf.release()
          throw e
        }
        first = end
      }
      java.util.Arrays.fill(serialResendMessages, null)
    }
  }

  fun send(outMessage: V086Message, numToSend: Int = 5) {
    synchronized(sendMutex) {
      outMessage.messageNumber = getAndIncrementSendMessageNumber()
      var numToSend = numToSend
      val buf = combinedKailleraController.alloc().directBuffer(flags.v086BufferSize)
      try {
        lastMessageBuffer.add(outMessage)
        numToSend = lastMessageBuffer.fill(outMessages, numToSend)
        val outBundle = V086Bundle.Multi(outMessages, numToSend)
        stripFromProdBinary { logger.atFinest().log("<- TO P%d: %s", user.id, outMessage) }
        outBundle.writeTo(buf)
        combinedKailleraController.send(DatagramPacket(buf, remoteSocketAddress!!))
        clientResponseMeter?.mark()
      } catch (e: Throwable) {
        buf.release()
        throw e
      }
    }
  }

  init {
    resetGameDataCache()
  }

  companion object {
    private val logger = FluentLogger.forEnclosingClass()
  }
}
