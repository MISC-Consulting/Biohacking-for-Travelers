# api_service.py

from twisted.web import server, resource
from twisted.internet import reactor, defer
from twisted.internet.task import LoopingCall
from ctrader_open_api import Client, Protobuf, TcpProtocol, Auth, EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *
from google.protobuf.json_format import MessageToJson
import json
import sys

import os
from dotenv import load_dotenv

load_dotenv()
ct_port = int(os.getenv("CT_PORT", 8090))

from functions.helpers import log
logs_filename = "../api_service_logs.txt"

import requests  # Needed for refreshing token

class cTraderAPI:
    def __init__(self, hostType, appClientId, appClientSecret, accessToken, accountId):
        self.host = EndPoints.PROTOBUF_LIVE_HOST if hostType.lower() == "live" else EndPoints.PROTOBUF_DEMO_HOST
        self.appClientId = appClientId
        self.appClientSecret = appClientSecret
        self.accessToken = accessToken
        self.currentAccountId = int(accountId) if accountId else None
        self.client = Client(self.host, EndPoints.PROTOBUF_PORT, TcpProtocol)
        self.client.setConnectedCallback(self.connected)
        self.client.setDisconnectedCallback(self.disconnected)
        self.client.setMessageReceivedCallback(self.onMessageReceived)
        self.connected_event = False
        self.response = None
        self.tracked_prices = {}

        self.refreshing_token = False  # Prevent multiple simultaneous refresh attempts

        self.client.startService()

    def connected(self, client):
        log("[PROTOBUF] Connected to cTrader server", filename=logs_filename)
        log(f"[PROTOBUF] Host: {self.host}:{EndPoints.PROTOBUF_PORT}", filename=logs_filename)
        log(f"[PROTOBUF] Client ID: {self.appClientId[:20]}...", filename=logs_filename)

        # Create and send application authentication request
        request = ProtoOAApplicationAuthReq()
        request.clientId = self.appClientId
        request.clientSecret = self.appClientSecret

        log("[PROTOBUF] Sending ProtoOAApplicationAuthReq", filename=logs_filename)
        log(f"[PROTOBUF] Request details: clientId={self.appClientId[:20]}..., clientSecret=***", filename=logs_filename)

        deferred = client.send(request)
        deferred.addCallbacks(self.onApplicationAuthRes, self.onError)

    def onApplicationAuthRes(self, message):
        log("[PROTOBUF] ✅ Application authentication successful", filename=logs_filename)

        # Extract and log response details
        try:
            response = Protobuf.extract(message)
            log(f"[PROTOBUF] Auth response: {response}", filename=logs_filename)
        except Exception as e:
            log(f"[PROTOBUF] Could not extract auth response: {e}", filename=logs_filename)

        log("[PROTOBUF] Proceeding to account authentication...", filename=logs_filename)
        self.sendProtoOAAccountAuthReq()

    def disconnected(self, client, reason):
        log(f"Disconnected: {reason}", filename=logs_filename)
        self.connected_event = False

    def onMessageReceived(self, client, message):
        extracted_message = Protobuf.extract(message)
        message_type = type(extracted_message).__name__
        #log(f"[PROTOBUF] 📨 Message received: {message_type}", filename=logs_filename)

        # Log detailed message content for debugging (except heartbeats to reduce noise)
        if not isinstance(extracted_message, ProtoHeartbeatEvent):
            try:
                # Convert to JSON for readable logging
                message_json = MessageToJson(extracted_message)
                #log(f"[PROTOBUF] Message content: {message_json}", filename=logs_filename)
            except Exception as e:
                log(f"[PROTOBUF] Could not convert message to JSON: {e}", filename=logs_filename)
                log(f"[PROTOBUF] Raw message: {extracted_message}", filename=logs_filename)

        # Check for token-expiration-related errors or invalidation events:
        if isinstance(extracted_message, ProtoOAErrorRes):
            error_code = extracted_message.errorCode
            description = extracted_message.description.lower() if extracted_message.description else ""

            # Common error codes indicating token/authorization issues:
            token_expired_codes = ["OA_AUTH_TOKEN_EXPIRED", "CH_ACCESS_TOKEN_INVALID"]
            # Sometimes "INVALID_REQUEST" with a certain description also indicates re-auth needed.
            if (
                    error_code in token_expired_codes
                    # or (error_code == "INVALID_REQUEST" and ("not authorized" in description or "access token has been expired" in description))
            ):
                log("Detected token/authorization error, attempting token refresh...", filename=logs_filename)
                self.refresh_token()

        elif isinstance(extracted_message, ProtoOAAccountsTokenInvalidatedEvent):
            log("Detected ProtoOAAccountsTokenInvalidatedEvent, attempting token refresh...", filename=logs_filename)
            self.refresh_token()
        # >>> END ADDED CODE <<<

        if isinstance(extracted_message, ProtoOASpotEvent):
            symbol_id = extracted_message.symbolId
            bid = extracted_message.bid
            ask = extracted_message.ask
            if bid != 0 and ask != 0:
                self.tracked_prices[symbol_id] = {
                    'current_bid': bid,
                    'current_ask': ask
                }
                #log(f"new ask price: {ask}", filename=logs_filename)
        #else:
            #log(f"Message received:\n {Protobuf.extract(message)}", filename=logs_filename)
        # Existing processing of other messages can remain here

    def onError(self, failure):
        print(f"Error: {failure}", filename=logs_filename)
        raise failure

    #######################  (Get Account List)
    def sendProtoOAGetAccountListByAccessTokenReq(self):
        request = ProtoOAGetAccountListByAccessTokenReq()
        request.accessToken = self.accessToken
        deferred = self.client.send(request)
        deferred.addCallbacks(self.onGetAccountListRes, self.onError)
        return deferred

    def onGetAccountListRes(self, message):
        response = Protobuf.extract(message)
        log(f"Account List: {response}", filename=logs_filename)
        self.response = response

    def setCurrentAccountId(self, accountId):
        self.currentAccountId = int(accountId)
        self.sendProtoOAAccountAuthReq()

    def sendProtoOAAccountAuthReq(self):
        request = ProtoOAAccountAuthReq()
        request.ctidTraderAccountId = int(self.currentAccountId)
        request.accessToken = self.accessToken
        deferred = self.client.send(request)
        deferred.addCallbacks(self.onAccountAuthRes, self.onError)

    def onAccountAuthRes(self, message):
        log("Account authenticated", filename=logs_filename)
        # Proceed with account-specific actions

    #######################  (Assets List)
    def sendProtoOAAssetListReq(self):
        request = ProtoOAAssetListReq()
        request.ctidTraderAccountId = int(self.currentAccountId)
        deferred = self.client.send(request)
        deferred.addCallbacks(self.onAssetListRes, self.onError)
        return deferred

    def onAssetListRes(self, message):
        response = Protobuf.extract(message)
        log(f"Asset List: {response}", filename=logs_filename)
        self.response = response

    #######################  (New Market Order)
    def sendNewMarketOrder(self, symbolId, tradeSide, volume, relativeStopLoss, relativeTakeProfit, trailingStopLoss):
        request = ProtoOANewOrderReq()
        request.ctidTraderAccountId = int(self.currentAccountId)
        request.symbolId = int(symbolId)
        request.orderType = ProtoOAOrderType.MARKET
        request.tradeSide = ProtoOATradeSide.Value(tradeSide.upper())
        request.volume = int(volume)
        request.relativeStopLoss = int(relativeStopLoss)
        
        if relativeTakeProfit:
            request.relativeTakeProfit = int(relativeTakeProfit)
        
        if trailingStopLoss == "True":
            trailingStopLoss = True
        elif trailingStopLoss == "False":
            trailingStopLoss = False
        else:
            trailingStopLoss = bool(trailingStopLoss)
        
        request.trailingStopLoss = trailingStopLoss
        deferred = self.client.send(request)
        deferred.addCallbacks(self.onNewOrderRes, self.onError)
        return deferred

    def onNewOrderRes(self, message):
        response = Protobuf.extract(message)
        log(f"Order Response: {response}", filename=logs_filename)
        self.response = response
        
    def onNewAmendmentRes(self, message):
        response = Protobuf.extract(message)
        log(f"Amendment Response: {response}", filename=logs_filename)
        self.response = response
        
    def sendAmendPositionSLTPReq(self, positionId, stopLoss, takeProfit, trailingStopLoss):
        
        print("Attempting position ammendment (Messaging stage)")
        
        request = ProtoOAAmendPositionSLTPReq()
        request.ctidTraderAccountId = int(self.currentAccountId)
        request.positionId = int(positionId)
        
        request.stopLoss = float(stopLoss)
        
        if takeProfit:
            request.takeProfit = float(takeProfit)
        
        if trailingStopLoss == "True":
            trailingStopLoss = True
        elif trailingStopLoss == "False":
            trailingStopLoss = False
        else:
            trailingStopLoss = bool(trailingStopLoss)
        
        request.trailingStopLoss = trailingStopLoss
        deferred = self.client.send(request)
        deferred.addCallbacks(self.onNewAmendmentRes, self.onError)
        return deferred
    
    def onGetExpectedMarginRes(self, message):
        response = Protobuf.extract(message)
        self.response = response
    
    def sendProtoOAGetExpectedMarginReq(self, symbolId, volume_end_step):
        
        print("Getting expected margin requirement... (Messaging stage)")
        volume = list(range(1, volume_end_step + 1))
        
        request = ProtoOAExpectedMarginReq()
        request.ctidTraderAccountId = int(self.currentAccountId)
        request.symbolId = int(symbolId)
        request.volume.extend(volume)
        deferred = self.client.send(request)
        deferred.addCallbacks(self.onGetExpectedMarginRes, self.onError)
        return deferred
        
    #######################  (Close Position)
    def sendProtoOAClosePositionReq(self, positionId, volume):
        request = ProtoOAClosePositionReq()
        request.ctidTraderAccountId = int(self.currentAccountId)
        request.positionId = int(positionId)
        request.volume = volume
        deferred = self.client.send(request)
        deferred.addCallbacks(self.onClosePositionRes, self.onError)
        return deferred

    def onClosePositionRes(self, message):
        response = Protobuf.extract(message)
        log(f"Close Position Response: {response}", filename=logs_filename)
        self.response = response

    def sendProtoOAGetPositionUnrealizedPnLReq(self):
        request = ProtoOAGetPositionUnrealizedPnLReq()
        request.ctidTraderAccountId = int(self.currentAccountId)
        deferred = self.client.send(request)
        deferred.addCallbacks(self.onGetPositionsRes, self.onError)
        return deferred

    def onGetPositionsRes(self, message):
        response = Protobuf.extract(message)
        log(f"Positions Held:{response}", filename=logs_filename)
        self.response = response

    #######################  (Reconcile)
    def sendProtoOAReconcileReq(self):
        request = ProtoOAReconcileReq()
        request.ctidTraderAccountId = int(self.currentAccountId)
        deferred = self.client.send(request)
        deferred.addCallbacks(self.onReconcileRes, self.onError)
        return deferred

    def onReconcileRes(self, message):
        response = Protobuf.extract(message)
        #log(f"Reconcile Response:{response}", filename=logs_filename)
        self.response = response

    #######################  (Get Trendbars)
    def sendProtoOAGetTrendbarsReq(self, symbolId, period, fromTimestamp, toTimestamp, count):
        request = ProtoOAGetTrendbarsReq()
        request.ctidTraderAccountId = int(self.currentAccountId)
        request.symbolId = int(symbolId)
        try:
            request.period = ProtoOATrendbarPeriod.Value(period.upper())
        except ValueError:
            raise ValueError(f"Invalid period value: {period}")
        request.fromTimestamp = int(fromTimestamp)
        request.toTimestamp = int(toTimestamp)
        request.count = int(count)
        deferred = self.client.send(request)
        deferred.addCallbacks(self.onGetTrendbarsRes, self.onError)
        return deferred

    def onGetTrendbarsRes(self, message):
        response = Protobuf.extract(message)
        #log(f"Trendbars: {response}", filename=logs_filename)
        self.response = response

    #######################  (Get Trader Account)
    def sendProtoOATraderReq(self):
        request = ProtoOATraderReq()
        request.ctidTraderAccountId = int(self.currentAccountId)
        deferred = self.client.send(request)
        deferred.addCallbacks(self.onTraderRes, self.onError)
        return deferred

    def onTraderRes(self, message):
        response = Protobuf.extract(message)
        log(f"Trader Account:{response}", filename=logs_filename)
        self.response = response

    #######################  (Symbols List)
    def sendProtoOASymbolsListReq(self):
        log("[PROTOBUF] 📋 Requesting symbols list", filename=logs_filename)
        log(f"[PROTOBUF] Account ID: {self.currentAccountId}", filename=logs_filename)

        request = ProtoOASymbolsListReq()
        request.ctidTraderAccountId = int(self.currentAccountId)

        log(f"[PROTOBUF] Sending ProtoOASymbolsListReq for account {self.currentAccountId}", filename=logs_filename)

        deferred = self.client.send(request)
        deferred.addCallbacks(self.onSymbolsListRes, self.onError)
        return deferred

    def onSymbolsListRes(self, message):
        response = Protobuf.extract(message)
        log("[PROTOBUF] ✅ Symbols list response received", filename=logs_filename)

        try:
            # Log detailed symbol information
            if hasattr(response, 'symbol'):
                symbols = response.symbol
                log(f"[PROTOBUF] Number of symbols received: {len(symbols)}", filename=logs_filename)

                # Look for BTCUSD specifically
                btc_symbols = [s for s in symbols if hasattr(s, 'symbolName') and s.symbolName == 'BTCUSD']
                if btc_symbols:
                    btc_symbol = btc_symbols[0]
                    log(f"[PROTOBUF] ✅ BTCUSD found - ID: {btc_symbol.symbolId}, Name: {btc_symbol.symbolName}", filename=logs_filename)
                else:
                    log("[PROTOBUF] ⚠️ BTCUSD not found in symbols list", filename=logs_filename)
                    # Log first few symbol names for debugging
                    sample_names = [s.symbolName for s in symbols[:10] if hasattr(s, 'symbolName')]
                    log(f"[PROTOBUF] Sample symbol names: {sample_names}", filename=logs_filename)

                # Convert to format expected by the bot
                symbol_list = []
                for symbol in symbols:
                    if hasattr(symbol, 'symbolId') and hasattr(symbol, 'symbolName'):
                        symbol_list.append({
                            'symbolId': symbol.symbolId,
                            'symbolName': symbol.symbolName
                        })

                log(f"[PROTOBUF] Converted {len(symbol_list)} symbols to bot format", filename=logs_filename)
                self.response = {'symbol': symbol_list}
            else:
                log("[PROTOBUF] ❌ No 'symbol' attribute in response", filename=logs_filename)
                log(f"[PROTOBUF] Response attributes: {dir(response)}", filename=logs_filename)
                self.response = response

        except Exception as e:
            log(f"[PROTOBUF] ❌ Error processing symbols response: {e}", filename=logs_filename)
            self.response = response

    def sendProtoOASubscribeSpotsReq(self, symbol_id):
        request = ProtoOASubscribeSpotsReq()
        request.ctidTraderAccountId = int(self.currentAccountId)
        request.symbolId.append(int(symbol_id))
        deferred = self.client.send(request)
        deferred.addCallbacks(self.onSubscribeSpotsRes, self.onError)
        return deferred

    def onSubscribeSpotsRes(self, message):
        response = Protobuf.extract(message)
        log(f"Subscribed to spots for symbol_id(s). {response}", filename=logs_filename)
        self.response = response

    def refresh_token(self):
        """
        Attempt to refresh the expired token using the refresh token
        saved in 'secrets/client_credentials.json'.
        Update self.accessToken and the credentials file on success,
        then re-authenticate the application and account.
        """
        if self.refreshing_token:
            # Prevent multiple refresh attempts at the same time
            return

        self.refreshing_token = True
        try:
            with open("secrets/client_credentials.json", "r") as creds_file:
                creds = json.load(creds_file)

            refresh_token = creds.get("refreshToken")
            if not refresh_token:
                log("No refresh token found in credentials. Cannot refresh.", filename=logs_filename)
                self.refreshing_token = False
                return

            TOKEN_URL = "https://openapi.ctrader.com/apps/token"
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.appClientId,
                "client_secret": self.appClientSecret
            }
            headers = {"Content-Type": "application/x-www-form-urlencoded"}

            response = requests.post(TOKEN_URL, data=payload, headers=headers, timeout=50)
            if response.status_code == 200:
                token_data = response.json()
                log(f"Token refreshed successfully: {token_data}", filename=logs_filename)

                # Update in-memory access token
                new_access_token = token_data.get("access_token")
                new_refresh_token = token_data.get("refresh_token")

                if new_access_token:
                    self.accessToken = new_access_token
                    creds["accessToken"] = new_access_token

                # If the server returns a new refresh token, update it
                if new_refresh_token:
                    creds["refreshToken"] = new_refresh_token

                # Save updated credentials back to file
                with open("secrets/client_credentials.json", "w") as wfile:
                    json.dump(creds, wfile, indent=4)

                # Re-authenticate the application and account
                self.reauthenticate()
            else:
                log(f"Failed to refresh token: {response.text}", filename=logs_filename)
        except Exception as e:
            log(f"Exception during token refresh: {e}", filename=logs_filename)
        finally:
            self.refreshing_token = False

    def reauthenticate(self):
        """
        Perform the same steps as the initial connection does:
        - Application Auth
        - Then in onApplicationAuthRes => sendProtoOAAccountAuthReq
        """
        log("Re-authenticating application after token refresh...", filename=logs_filename)
        # Re-do the application auth (similar to what 'connected()' does).
        request = ProtoOAApplicationAuthReq()
        request.clientId = self.appClientId
        request.clientSecret = self.appClientSecret
        deferred = self.client.send(request)
        deferred.addCallbacks(self.onApplicationAuthRes, self.onError)
    # >>> END ADDED CODE <<<


class APIResource(resource.Resource):
    isLeaf = True

    def __init__(self, api):
        super().__init__()
        self.api = api

    def render_GET(self, request):
        #log('[render_GET Started]', filename=logs_filename)
        action = request.args.get(b'action', [b''])[0].decode('utf-8')
        try:
            if action in [
                'get_account_list',
                'assets_list',
                'new_market_order',
                'close_position',
                'get_positions',
                'reconcile',
                'get_trend_bars',
                'get_trader_account',
                'symbols_list',
                'get_current_price',
                'amend_position_sltp',
                'get_expected_margin'
            ]:
                self.api.response = None
                if action == 'get_account_list':
                    self.api.sendProtoOAGetAccountListByAccessTokenReq()
                elif action == 'assets_list':
                    self.api.sendProtoOAAssetListReq()
                elif action == 'new_market_order':
                    symbolId = request.args.get(b'symbolId', [b''])[0].decode('utf-8')
                    tradeSide = request.args.get(b'tradeSide', [b''])[0].decode('utf-8')
                    volume = request.args.get(b'volume', [b''])[0].decode('utf-8')
                    relativeStopLoss = request.args.get(b'relativeStopLoss', [b''])[0].decode('utf-8')
                    relativeTakeProfit = request.args.get(b'relativeTakeProfit', [b''])[0].decode('utf-8')
                    
                    if int(relativeTakeProfit) < 0:
                        relativeTakeProfit = None # No take profit set e.g. trailing stop only
                    
                    trailingStopLoss = request.args.get(b'trailingStopLoss', [b''])[0].decode('utf-8')
                    self.api.sendNewMarketOrder(symbolId, tradeSide, volume, relativeStopLoss, relativeTakeProfit, trailingStopLoss)
                elif action == 'amend_position_sltp':
                    
                    print("Attempting position amendment... (ProtoBuf packaging)")
                    
                    positionId = request.args.get(b'positionId', [b''])[0].decode('utf-8')
                    stopLoss = request.args.get(b'stopLoss', [b''])[0].decode('utf-8')
                    takeProfit = request.args.get(b'takeProfit', [b''])[0].decode('utf-8')
                    
                    if int(relativeTakeProfit) < 0:
                        relativeTakeProfit = None # No take profit set e.g. trailing stop only
                    
                    trailingStopLoss = request.args.get(b'trailingStopLoss', [b''])[0].decode('utf-8')
                    self.api.sendAmendPositionSLTPReq(positionId, stopLoss, takeProfit, trailingStopLoss)
                elif action == 'close_position':
                    positionId = request.args.get(b'positionId', [b''])[0].decode('utf-8')
                    volume = request.args.get(b'volume', [b''])[0].decode('utf-8')
                    self.api.sendProtoOAClosePositionReq(positionId, volume)
                elif action == 'get_positions':
                    self.api.sendProtoOAGetPositionUnrealizedPnLReq()
                elif action == 'reconcile':
                    self.api.sendProtoOAReconcileReq()
                elif action == 'get_trend_bars':
                    symbolId = request.args.get(b'symbolId', [b''])[0].decode('utf-8')
                    period = request.args.get(b'period', [b''])[0].decode('utf-8')
                    fromTimestamp = request.args.get(b'fromTimestamp', [b''])[0].decode('utf-8')
                    toTimestamp = request.args.get(b'toTimestamp', [b''])[0].decode('utf-8')
                    count = request.args.get(b'count', [b''])[0].decode('utf-8')
                    self.api.sendProtoOAGetTrendbarsReq(symbolId, period, fromTimestamp, toTimestamp, count)
                elif action == 'get_trader_account':
                    self.api.sendProtoOATraderReq()
                elif action == 'symbols_list':
                    self.api.sendProtoOASymbolsListReq()
                elif action == "get_expected_margin":
                    
                    print("Getting expected margin requirement...")
                    
                    symbol_id = request.args.get(b'symbolId', [b''])[0].decode('utf-8')
                    symbol_id = int(symbol_id)
                    
                    volume = request.args.get(b'volume', [b''])[0].decode('utf-8')
                    volume = int(volume)
                    
                    self.api.sendProtoOAGetExpectedMarginReq(symbol_id, volume)
                    
                elif action == 'get_current_price':
                    
                    symbol_id = request.args.get(b'symbolId', [b''])[0].decode('utf-8')
                    symbol_id = int(symbol_id)
            
                    def send_response_if_tracked():
                        if symbol_id in self.api.tracked_prices and self.api.tracked_prices[symbol_id]:
                            response_data = self.api.tracked_prices[symbol_id]
                            try:
                                response_json = json.dumps(response_data)
                                request.setHeader(b"content-type", b"application/json")
                                request.write(response_json.encode('utf-8'))
                            except Exception as e:
                                request.setHeader(b"content-type", b"text/plain")
                                request.write(f"Error sending response: {e}".encode('utf-8'))
                            finally:
                                if hasattr(self, '_current_timeout') and self._current_timeout.active():
                                    self._current_timeout.cancel()
                                if hasattr(self, '_current_polling') and self._current_polling.running:
                                    self._current_polling.stop()
                                request.finish()
                                self.api.response = None

                    if symbol_id in self.api.tracked_prices and self.api.tracked_prices[symbol_id]:
                        send_response_if_tracked()
                        return server.NOT_DONE_YET
                    else:
                        self.api.sendProtoOASubscribeSpotsReq(symbol_id)
                        # Start polling for response
                        polling = LoopingCall(send_response_if_tracked)
                        polling_deferred = polling.start(0.25, now=False)
                        # Set timeout for 90 seconds
                        timeout = reactor.callLater(90, self.handle_timeout, request, polling)

                        # Attach a callback to stop polling and timeout if response is received
                        d = defer.Deferred()
                        d.addBoth(lambda _: None)  # Prevents warnings
                        self._current_polling = polling
                        self._current_timeout = timeout
                        return server.NOT_DONE_YET

                # Start polling for response
                polling = LoopingCall(self.check_response, request)
                polling_deferred = polling.start(0.25, now=False)
                # Set timeout for 90 seconds
                timeout = reactor.callLater(90, self.handle_timeout, request, polling)

                # Attach a callback to stop polling and timeout if response is received
                d = defer.Deferred()
                d.addBoth(lambda _: None)  # Prevents warnings
                self._current_polling = polling
                self._current_timeout = timeout
                return server.NOT_DONE_YET

            elif action == 'set_account':
                accountId = request.args.get(b'accountId', [b''])[0].decode('utf-8')
                self.api.setCurrentAccountId(accountId)
                request.setHeader(b"content-type", b"text/plain")
                self.api.response = None
                return b"Account set"
            else:
                request.setHeader(b"content-type", b"text/plain")
                return b'Invalid action'
        except KeyError as ke:
            request.setHeader(b"content-type", b"text/plain")
            response = f"Missing parameter: {ke}".encode('utf-8')
            return response
        except ValueError as ve:
            request.setHeader(b"content-type", b"text/plain")
            response = f"Invalid value: {ve}".encode('utf-8')
            return response
        except Exception as e:
            request.setHeader(b"content-type", b"text/plain")
            response = f"Error processing request: {e}".encode('utf-8')
            return response

    def check_response(self, request):
        if self.api.response:
            try:
                # Cancel the timeout since response is received
                if hasattr(self, '_current_timeout') and self._current_timeout.active():
                    self._current_timeout.cancel()
                # Stop the polling
                if hasattr(self, '_current_polling') and self._current_polling.running:
                    self._current_polling.stop()
                self.send_response(request)
            except Exception as e:
                request.setHeader(b"content-type", b"text/plain")
                response = f"Error sending response: {e}".encode('utf-8')
                try:
                    request.write(response)
                    request.finish()
                except Exception:
                    pass

                log(f"Exception in check_response: {e}", filename=logs_filename)

    def handle_timeout(self, request, polling):
        if polling.running:
            polling.stop()
        try:
            request.setHeader(b"content-type", b"text/plain")
            request.write(b"No response received within the timeout period.")
            request.finish()
        except Exception as e:
            log(f"Exception in handle_timeout: {e}", filename=logs_filename)

    def send_response(self, request):
        if self.api.response:
            try:
                # Check if response is already a dict (converted symbols) or a protobuf message
                if isinstance(self.api.response, dict):
                    response_data = json.dumps(self.api.response)
                else:
                    response_data = MessageToJson(self.api.response)
                request.setHeader(b"content-type", b"application/json")
                request.write(response_data.encode('utf-8'))
            except Exception as e:
                request.setHeader(b"content-type", b"text/plain")
                request.write(f"Error converting response to JSON: {e}".encode('utf-8'))
        else:
            request.setHeader(b"content-type", b"text/plain")
            request.write(b"No response received yet")
        request.finish()
        self.api.response = None


# Load your credentials from a file or define them here
# with open("secrets/my_credentials.json") as credentialsFile:
#     credentials = json.load(credentialsFile)

with open("secrets/client_credentials.json") as credentialsFile:
    credentials = json.load(credentialsFile)

hostType = credentials['hostType']
appClientId = credentials['appClientId']
appClientSecret = credentials['appClientSecret']
accessToken = credentials['accessToken']
accountId = credentials['accountId']

api = cTraderAPI(hostType, appClientId, appClientSecret, accessToken, accountId)

# Set up the site and listen on a port
site = server.Site(APIResource(api))

print(f"[CTRADER] Listening on port: {ct_port}")

reactor.listenTCP(ct_port, site, interface = "127.0.0.1")
reactor.run()