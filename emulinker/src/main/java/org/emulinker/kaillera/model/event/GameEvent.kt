package org.emulinker.kaillera.model.event

import io.netty.buffer.ByteBuf
import org.emulinker.kaillera.model.KailleraGame
import org.emulinker.kaillera.model.KailleraUser

sealed interface GameEvent : KailleraEvent {
  val game: KailleraGame?
}

data class GameInfoEvent(
  override val game: KailleraGame,
  val message: String,
  val toUser: KailleraUser? = null,
) : GameEvent

data class GameChatEvent(
  override val game: KailleraGame,
  val user: KailleraUser,
  val message: String,
) : GameEvent

/**
 * Carries an opaque retry-connect signal from [fromUser] to the other player(s) in [game]. Unlike
 * [GameInfoEvent] (broadcast to everyone, including the sender, for server announcements), this
 * always has a concrete sender and is meant for "everyone else in the room" - the receiving
 * [org.emulinker.kaillera.controller.v086.action.RetryConnectAction] skips [fromUser] itself
 * rather than echoing the signal back to its own author.
 */
data class RetryConnectEvent(
  override val game: KailleraGame,
  val fromUser: KailleraUser,
  val subtype: Byte,
  val payload: ByteArray,
) : GameEvent

data class AllReadyEvent(override val game: KailleraGame) : GameEvent

data class GameDesynchEvent(override val game: KailleraGame, val message: String) : GameEvent

// Why is there a gamedata and gamedataevent
data class GameDataEvent(override val game: KailleraGame, val data: ByteBuf) : GameEvent

data class GameStartedEvent(override val game: KailleraGame) : GameEvent

data class UserJoinedGameEvent(override val game: KailleraGame, val user: KailleraUser) : GameEvent

// If game is null, I think that means the user quit the whole server.
data class UserQuitGameEvent(override val game: KailleraGame?, val user: KailleraUser) : GameEvent

data class PlayerDesynchEvent(
  override val game: KailleraGame,
  val user: KailleraUser,
  val message: String,
) : GameEvent

data class UserDroppedGameEvent(
  override val game: KailleraGame,
  val user: KailleraUser,
  val playerNumber: Int,
) : GameEvent
