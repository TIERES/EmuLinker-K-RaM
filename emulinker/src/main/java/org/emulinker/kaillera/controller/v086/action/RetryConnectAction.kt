package org.emulinker.kaillera.controller.v086.action

import com.google.common.flogger.FluentLogger
import org.emulinker.kaillera.controller.messaging.MessageFormatException
import org.emulinker.kaillera.controller.v086.V086ClientHandler
import org.emulinker.kaillera.controller.v086.protocol.RetryConnectNotification
import org.emulinker.kaillera.controller.v086.protocol.RetryConnectRequest
import org.emulinker.kaillera.model.event.RetryConnectEvent

/**
 * Relays retry-connect signals (opaque to this server - see [RetryConnectEvent]) between the
 * clients in a game room. Modeled on [DropGameAction]: receives a [RetryConnectRequest] from one
 * client, hands it to [org.emulinker.kaillera.model.KailleraGame.retryConnectRelay], and forwards
 * the resulting [RetryConnectEvent] to every other player in the room as a
 * [RetryConnectNotification].
 */
class RetryConnectAction : V086Action<RetryConnectRequest>, V086GameEventHandler<RetryConnectEvent> {
  override fun toString() = "RetryConnectAction"

  override fun performAction(message: RetryConnectRequest, clientHandler: V086ClientHandler) {
    val game = clientHandler.user.game ?: return
    game.retryConnectRelay(clientHandler.user, message.subtype, message.payload)
  }

  override fun handleEvent(event: RetryConnectEvent, clientHandler: V086ClientHandler) {
    if (event.fromUser === clientHandler.user) return // don't echo back to the sender
    try {
      clientHandler.send(
        RetryConnectNotification(0, event.fromUser.name!!, event.subtype, event.payload)
      )
    } catch (e: MessageFormatException) {
      logger.atSevere().withCause(e).log("Failed to construct RetryConnect.Notification message")
    }
  }

  companion object {
    private val logger = FluentLogger.forEnclosingClass()
  }
}
