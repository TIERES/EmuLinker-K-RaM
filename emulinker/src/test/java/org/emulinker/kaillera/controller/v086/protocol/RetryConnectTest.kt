package org.emulinker.kaillera.controller.v086.protocol

class RetryConnectRequestTest : V086MessageTest<RetryConnect>() {
  override val message =
    RetryConnectRequest(messageNumber = MESSAGE_NUMBER, subtype = 1, payload = byteArrayOf())
  override val byteString = "00, 01, 00, 00"
  override val serializer = RetryConnect.RetryConnectSerializer
}

class RetryConnectRequestWithPayloadTest : V086MessageTest<RetryConnect>() {
  override val message =
    RetryConnectRequest(
      messageNumber = MESSAGE_NUMBER,
      subtype = 2,
      payload = byteArrayOf(9, 8, 7),
    )
  override val byteString = "00, 02, 03, 00, 09, 08, 07"
  override val serializer = RetryConnect.RetryConnectSerializer
}

class RetryConnectNotificationTest : V086MessageTest<RetryConnect>() {
  override val message =
    RetryConnectNotification(
      messageNumber = MESSAGE_NUMBER,
      fromUsername = "nue",
      subtype = 2,
      payload = byteArrayOf(9, 8, 7),
    )
  override val byteString = "6E, 75, 65, 00, 02, 03, 00, 09, 08, 07"
  override val serializer = RetryConnect.RetryConnectSerializer
}
