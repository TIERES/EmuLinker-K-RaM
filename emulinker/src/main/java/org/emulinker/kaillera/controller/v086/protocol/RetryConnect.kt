package org.emulinker.kaillera.controller.v086.protocol

import io.netty.buffer.ByteBuf
import java.nio.ByteBuffer
import org.emulinker.kaillera.controller.v086.V086Utils
import org.emulinker.kaillera.controller.v086.V086Utils.getNumBytesPlusStopByte
import org.emulinker.util.EmuUtil
import org.emulinker.util.EmuUtil.readString
import org.emulinker.util.UnsignedUtil.putUnsignedShort

/**
 * Carries an opaque "retry-connect" payload between the two clients in a game room, relayed
 * unmodified by the server (the same way [GameData] relays controller-state bytes without
 * interpreting them) - [subtype] and [payload] are meaningful only to the client (kailleraclient.dll),
 * not to this server.
 *
 * Message type ID: `0x18`.
 */
sealed class RetryConnect : V086Message() {
  override val messageTypeId = ID

  abstract val subtype: Byte
  abstract val payload: ByteArray

  override val bodyBytes: Int
    get() =
      when (this) {
        is RetryConnectRequest -> REQUEST_USERNAME
        is RetryConnectNotification -> fromUsername
      }.getNumBytesPlusStopByte() + V086Utils.Bytes.SINGLE_BYTE + V086Utils.Bytes.SHORT + payload.size

  override fun writeBodyTo(buffer: ByteBuffer) {
    RetryConnectSerializer.write(buffer, this)
  }

  override fun writeBodyTo(buffer: ByteBuf) {
    RetryConnectSerializer.write(buffer, this)
  }

  companion object {
    const val ID: Byte = 0x18

    private const val REQUEST_USERNAME = ""
  }

  object RetryConnectSerializer : MessageSerializer<RetryConnect> {
    override val messageTypeId: Byte = ID

    override fun read(buffer: ByteBuf, messageNumber: Int): Result<RetryConnect> {
      if (buffer.readableBytes() < 2) {
        return parseFailure("Failed byte count validation!")
      }
      val userName = buffer.readString()
      if (buffer.readableBytes() < 3) {
        return parseFailure("Failed byte count validation!")
      }
      val subtype = buffer.readByte()
      val payloadSize = buffer.readUnsignedShortLE()
      if (payloadSize > buffer.readableBytes()) {
        return parseFailure("Invalid RetryConnect format: payloadSize = $payloadSize")
      }
      val payload = ByteArray(payloadSize)
      buffer.readBytes(payload)
      return Result.success(
        if (userName == REQUEST_USERNAME) {
          RetryConnectRequest(messageNumber, subtype, payload)
        } else {
          RetryConnectNotification(messageNumber, userName, subtype, payload)
        }
      )
    }

    override fun write(buffer: ByteBuf, message: RetryConnect) {
      EmuUtil.writeString(
        buffer,
        when (message) {
          is RetryConnectRequest -> REQUEST_USERNAME
          is RetryConnectNotification -> message.fromUsername
        },
      )
      buffer.writeByte(message.subtype.toInt())
      buffer.writeShortLE(message.payload.size)
      buffer.writeBytes(message.payload)
    }

    fun write(buffer: ByteBuffer, message: RetryConnect) {
      EmuUtil.writeString(
        buffer,
        when (message) {
          is RetryConnectRequest -> REQUEST_USERNAME
          is RetryConnectNotification -> message.fromUsername
        },
      )
      buffer.put(message.subtype)
      buffer.putUnsignedShort(message.payload.size)
      buffer.put(message.payload)
    }
  }
}

/** Sent by a client to relay a retry-connect signal to the other player(s) in its game room. */
data class RetryConnectRequest(
  override var messageNumber: Int,
  override val subtype: Byte,
  override val payload: ByteArray,
) : RetryConnect(), ClientMessage {
  init {
    require(payload.size <= 0xFFFF) { "payload size out of range: ${payload.size}" }
  }

  // Netty ByteBuf-backed byte arrays don't get free structural equality/hashCode from `data class`.
  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other !is RetryConnectRequest) return false
    return messageNumber == other.messageNumber &&
      subtype == other.subtype &&
      payload.contentEquals(other.payload)
  }

  override fun hashCode(): Int {
    var result = messageNumber
    result = 31 * result + subtype
    result = 31 * result + payload.contentHashCode()
    return result
  }
}

/** Sent by the server to relay [RetryConnectRequest] from [fromUsername] to one other player. */
data class RetryConnectNotification(
  override var messageNumber: Int,
  val fromUsername: String,
  override val subtype: Byte,
  override val payload: ByteArray,
) : RetryConnect(), ServerMessage {
  init {
    require(fromUsername.isNotBlank()) { "fromUsername cannot be blank" }
    require(payload.size <= 0xFFFF) { "payload size out of range: ${payload.size}" }
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (other !is RetryConnectNotification) return false
    return messageNumber == other.messageNumber &&
      fromUsername == other.fromUsername &&
      subtype == other.subtype &&
      payload.contentEquals(other.payload)
  }

  override fun hashCode(): Int {
    var result = messageNumber
    result = 31 * result + fromUsername.hashCode()
    result = 31 * result + subtype
    result = 31 * result + payload.contentHashCode()
    return result
  }
}
