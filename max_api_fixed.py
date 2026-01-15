"""
MaxAPI с исправленным механизмом реконнекта.
Основано на MaxBridge 1.8.0, исправлена логика переподключения.
"""

import tornado.ioloop
import tornado.websocket
import tornado.gen
import json
import time
import threading
import itertools
import signal
import requests
import io
import logging
from tornado.httpclient import HTTPRequest


class MaxAPI:
    """
    Python api wrapper for max messenger с исправленным реконнектом
    """

    OPCODE_MAP = {
        'HEARTBEAT': 1,
        'HANDSHAKE': 6,
        'SEND_VERIFY_CODE': 17,
        'CHECK_VERIFY_CODE': 18,
        'AUTHENTICATE': 19,
        'AUTH_CONFIRM': 20,
        'GET_CONTACT_DETAILS': 32,
        'FIND_BY_PHONE_NUMBER': 46,
        'GET_HISTORY': 49,
        'MARK_AS_READ': 50,
        'SEND_MESSAGE': 64,
        'SUBSCRIBE_TO_CHAT': 75,
    }

    def __init__(self, auth_token: str = None, on_event=None, auto_reconnect: bool = True):
        """
        Initializes the MaxAPI instance.

        Args:
            auth_token: The authentication token for the session.
            on_event: A callback function to handle server-push events.
            auto_reconnect: If True, automatically reconnects on connection loss.
        """
        self.token = auth_token
        self.ws_url = "wss://ws-api.oneme.ru/websocket"
        self.user_agent = {
            "deviceType": "WEB", "locale": "ru", "deviceLocale": "ru",
            "osVersion": "Windows", "deviceName": "Firefox",
            "headerUserAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0",
            "appVersion": "25.7.13", "screen": "1080x1920 1.0x", "timezone": "Asia/Novosibirsk"
        }
        self.user = None
        self.chats = {}
        self.subscribed_chats = set()
        self.auto_reconnect = auto_reconnect

        self.ws = None
        self.ioloop = None
        self.ioloop_thread = None
        self.heartbeat_callback = None

        self.is_running = False
        self._should_reconnect = True  # Новый флаг для контроля реконнекта
        self.seq_counter = itertools.count()

        self.response_lock = threading.Lock()
        self.pending_responses = {}
        self.ready_event = threading.Event()

        self.on_event = on_event if callable(on_event) else self._default_on_event

        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except ValueError:
            pass

        self.logger = logging.getLogger("MaxAPI")

        self._start_ioloop()

        is_ready = self.ready_event.wait(timeout=20)
        if not is_ready:
            self.close()
            raise TimeoutError("Failed to connect to WebSocket within the timeout period.")

    def _signal_handler(self, signum, frame):
        self.logger.info(f"\nSignal {signum} received, initiating shutdown...")
        self.close()

    def _default_on_event(self, event_data):
        opcode = event_data.get("opcode")
        if opcode == 128:
            self.logger.info(f"\n[New Message Received] Event: {json.dumps(event_data, indent=2, ensure_ascii=False)}\n")
        elif opcode is not None:
            self.logger.info(f"\n[Server Event Received] Event (Opcode {opcode}): {json.dumps(event_data, indent=2, ensure_ascii=False)}\n")
        else:
            self.logger.info(f"\n[Unknown Event Received] Event: {json.dumps(event_data, indent=2, ensure_ascii=False)}\n")

    def _start_ioloop(self):
        if self.ioloop_thread is not None:
            return
        self.ioloop = tornado.ioloop.IOLoop()
        self.ioloop_thread = threading.Thread(target=self.ioloop.start, daemon=True)
        self.ioloop.add_callback(self._connect_and_run)
        self.ioloop_thread.start()

    @tornado.gen.coroutine
    def _connect_and_run(self):
        while self._should_reconnect:
            try:
                self.logger.info('Connecting...')
                request = HTTPRequest(
                    url=self.ws_url,
                    headers={
                        "Origin": "https://web.max.ru",
                        "User-Agent": self.user_agent["headerUserAgent"],
                        "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "websocket",
                        "Sec-Fetch-Site": "cross-site",
                    }
                )
                self.ws = yield tornado.websocket.websocket_connect(request)
                self.is_running = True
                self.logger.info("Connected to WebSocket.")
                self.ioloop.add_callback(self._listener_loop_async)
                yield self._handshake_async()

                if self.token:
                    yield self._authenticate_async()
                    self.logger.info("API is online and ready.")
                else:
                    self.logger.info("API is connected. Please authenticate using verification code methods.")

                self.heartbeat_callback = tornado.ioloop.PeriodicCallback(self._send_heartbeat, 3000)
                self.heartbeat_callback.start()

                self.ready_event.set()
                break
            except Exception as e:
                self.logger.warning(f"Connection failed: {e}. Retrying in 5 seconds...")
                yield tornado.gen.sleep(5)

    @tornado.gen.coroutine
    def _listener_loop_async(self):
        try:
            while self.is_running:
                message = yield self.ws.read_message()
                if message is None:
                    if self._should_reconnect and self.auto_reconnect:
                        self.logger.warning("Connection closed by server. Attempting to reconnect...")
                        yield self._reconnect_async()
                    break
                self._process_message(message)
        except tornado.websocket.WebSocketClosedError:
            if self._should_reconnect and self.auto_reconnect:
                self.logger.warning("Listener loop terminated: WebSocket closed by server. Reconnecting...")
                yield self._reconnect_async()
        except Exception as e:
            if self._should_reconnect and self.auto_reconnect:
                self.logger.error(f"An error occurred in the listener loop: {e}")
                yield self._reconnect_async()

    @tornado.gen.coroutine
    def _reconnect_async(self):
        """Исправленная логика реконнекта"""
        self.is_running = False
        if self.heartbeat_callback:
            self.heartbeat_callback.stop()

        if self.ws:
            try:
                self.ws.close()
            except:
                pass

        reconnect_delay = 2
        max_delay = 60

        while self._should_reconnect and self.auto_reconnect:
            yield tornado.gen.sleep(reconnect_delay)

            try:
                self.logger.info(f'Reconnecting (delay: {reconnect_delay}s)...')
                request = HTTPRequest(
                    url=self.ws_url,
                    headers={
                        "Origin": "https://web.max.ru",
                        "User-Agent": self.user_agent["headerUserAgent"],
                        "Sec-Fetch-Dest": "empty",
                        "Sec-Fetch-Mode": "websocket",
                        "Sec-Fetch-Site": "cross-site",
                    }
                )
                self.ws = yield tornado.websocket.websocket_connect(request)
                self.is_running = True
                self.logger.info("Reconnected to WebSocket.")

                self.ioloop.add_callback(self._listener_loop_async)
                yield self._handshake_async()

                if self.token:
                    yield self._authenticate_async()
                    self.logger.info("Re-authenticated successfully.")

                if self.heartbeat_callback:
                    self.heartbeat_callback.start()

                # Переподписываемся на чаты
                for chat_id in list(self.subscribed_chats):
                    try:
                        yield self.send_command_async(
                            self.OPCODE_MAP['SUBSCRIBE_TO_CHAT'],
                            {"chatId": int(chat_id), "subscribe": True}
                        )
                        self.logger.info(f"Resubscribed to chat {chat_id}")
                    except Exception as e:
                        self.logger.warning(f"Failed to resubscribe to chat {chat_id}: {e}")

                self.logger.info("Reconnection complete.")
                # Сбрасываем delay при успехе
                reconnect_delay = 2
                break

            except Exception as e:
                self.logger.error(f"Reconnection failed: {e}")
                # Exponential backoff
                reconnect_delay = min(reconnect_delay * 2, max_delay)

    def _process_message(self, message):
        try:
            data = json.loads(message)

            match data.get("cmd"):
                case 1:
                    seq_id = data.get("seq")
                    with self.response_lock:
                        pending_request = self.pending_responses.get(seq_id)

                    if pending_request:
                        original_opcode = pending_request.get("opcode")
                        if original_opcode != self.OPCODE_MAP['HEARTBEAT']:
                            self.logger.debug(f"API Response (for Opcode {original_opcode}, Seq {seq_id}): {json.dumps(data, ensure_ascii=False)}")

                        if "event" in pending_request:
                            pending_request["response"] = data
                            pending_request["event"].set()
                        elif "future" in pending_request:
                            with self.response_lock:
                                self.pending_responses.pop(seq_id, None)
                            pending_request["future"].set_result(data)
                case 0:
                    if self.on_event:
                        self.ioloop.run_in_executor(None, self.on_event, data)
                case 3:
                    self.logger.error(f"Received API error: {json.dumps(data, indent=4, ensure_ascii=False)}")
                case _:
                    self.logger.debug(f"Received unexpected API response: {json.dumps(data, indent=4, ensure_ascii=False)}")

        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    def close(self):
        if not self.is_running and self.ioloop is None:
            return
        self.logger.info("Closing connection...")
        self._should_reconnect = False  # Запрещаем реконнект
        self.is_running = False

        if self.ioloop:
            self.ioloop.add_callback(self._shutdown_async)

        if self.ioloop_thread and self.ioloop_thread.is_alive():
            self.ioloop_thread.join(timeout=5)

        self.ioloop = None
        self.ioloop_thread = None
        self.logger.info("Connection closed.")

    @tornado.gen.coroutine
    def _shutdown_async(self):
        if self.heartbeat_callback:
            self.heartbeat_callback.stop()
        if self.ws:
            self.ws.close()
        self.ioloop.call_later(0.1, self.ioloop.stop)

    @tornado.gen.coroutine
    def send_command_async(self, opcode: int, payload: dict, timeout: int = 10):
        if not self.is_running:
            raise ConnectionError("Not connected.")

        seq_id = next(self.seq_counter)
        command = {"ver": 11, "cmd": 0, "seq": seq_id, "opcode": opcode, "payload": payload}

        future = tornado.gen.Future()
        with self.response_lock:
            self.pending_responses[seq_id] = {"future": future, "opcode": opcode}

        try:
            yield self.ws.write_message(json.dumps(command))
            response = yield tornado.gen.with_timeout(
                self.ioloop.time() + timeout,
                future
            )
            raise tornado.gen.Return(response)
        except tornado.gen.TimeoutError:
            with self.response_lock:
                self.pending_responses.pop(seq_id, None)
            raise TimeoutError(f"Async request (opcode: {opcode}, seq: {seq_id}) timed out.")

    @tornado.gen.coroutine
    def _handshake_async(self):
        self.logger.info("Performing handshake...")
        payload = {"userAgent": self.user_agent, "deviceId": "asd"}
        yield self.send_command_async(self.OPCODE_MAP['HANDSHAKE'], payload)
        self.logger.info("Handshake successful.")

    @tornado.gen.coroutine
    def _authenticate_async(self):
        self.logger.info("Authenticating...")
        if not self.token:
            self.logger.error("Authentication failed: No token available.")
            return

        payload = {
            "interactive": True, "token": self.token,
            "chatsSync": 0, "contactsSync": 0, "presenceSync": 0,
            "draftsSync": 0, "chatsCount": 50
        }
        response = yield self.send_command_async(self.OPCODE_MAP['AUTHENTICATE'], payload)

        response_payload = response.get('payload', {})
        if response_payload.get('error'):
            raise ValueError(f"Auth Error: {response_payload.get('error')}")

        self.logger.info(f"Authentication successful. User: {response_payload['profile']['contact']['names'][0]['name']}")
        self.user = response_payload['profile']['contact']

        chats = {}
        if 'chats' in response_payload:
            for item in response_payload['chats']:
                item_id = str(item.get('id'))
                new_item = item.copy()
                del new_item['id']
                chats[item_id] = new_item
        self.chats = chats

    @tornado.gen.coroutine
    def _send_heartbeat(self):
        if not self.is_running:
            return
        try:
            self.ioloop.add_callback(self.ws.write_message, json.dumps({
                "ver": 11, "cmd": 0, "seq": next(self.seq_counter),
                "opcode": self.OPCODE_MAP['HEARTBEAT'],
                "payload": {"interactive": False}
            }))
        except tornado.websocket.WebSocketClosedError:
            self.logger.warning("Heartbeat failed: WebSocket is closed.")
            self.is_running = False
        except Exception as e:
            if self.is_running:
                self.logger.error(f"Heartbeat failed with error: {e}")
                self.is_running = False

    def send_command(self, opcode: int, payload: dict, wait_for_response: bool = True, timeout: int = 10):
        if not self.is_running:
            raise ConnectionError("Not connected. Cannot send command.")

        seq_id = next(self.seq_counter)
        command = {"ver": 11, "cmd": 0, "seq": seq_id, "opcode": opcode, "payload": payload}

        if not wait_for_response:
            self.ioloop.add_callback(self.ws.write_message, json.dumps(command))
            return None

        event = threading.Event()
        with self.response_lock:
            self.pending_responses[seq_id] = {"event": event, "response": None, "opcode": opcode}

        self.ioloop.add_callback(self.ws.write_message, json.dumps(command))

        is_set = event.wait(timeout)

        with self.response_lock:
            pending_request = self.pending_responses.pop(seq_id, None)

        if not is_set:
            raise TimeoutError(f"Request (opcode: {opcode}, seq: {seq_id}) timed out after {timeout} seconds.")
        if not pending_request:
            raise RuntimeError(f"Response for request (seq: {seq_id}) was lost.")

        return pending_request.get("response")

    def _finalize_authentication(self):
        auth_event = threading.Event()
        auth_result = [None]

        def _run_authenticate():
            try:
                future = self._authenticate_async()
                self.ioloop.add_future(future, lambda f: auth_event.set())
            except Exception as e:
                auth_result[0] = e
                auth_event.set()

        self.ioloop.add_callback(_run_authenticate)
        auth_event.wait()
        if auth_result[0] is not None:
            raise auth_result[0]

        self.logger.info("API is online and ready.")

    # --- Public API Methods ---

    def send_message(self, chat_id: str, text: str, reply_id: str = None, wait_for_response: bool = False):
        client_message_id = int(time.time() * 1000)
        payload = {
            "chatId": int(chat_id),
            "message": {"text": text, "cid": client_message_id, "elements": [], "attaches": []},
            "notify": True
        }
        if reply_id:
            payload["message"]["link"] = {"type": "REPLY", "messageId": int(reply_id)}

        self.logger.info(f"Sent message to chat {chat_id} with cid {client_message_id}")
        return self.send_command(self.OPCODE_MAP['SEND_MESSAGE'], payload, wait_for_response=wait_for_response)

    def get_history(self, chat_id: str, count: int = 30, from_timestamp: int = None):
        if from_timestamp is None:
            from_timestamp = int(time.time() * 1000)
        payload = {"chatId": int(chat_id), "from": from_timestamp, "forward": 0, "backward": count, "getMessages": True}
        return self.send_command(self.OPCODE_MAP['GET_HISTORY'], payload)

    def subscribe_to_chat(self, chat_id: str, subscribe: bool = True):
        payload = {"chatId": int(chat_id), "subscribe": subscribe}
        status = "Subscribed to" if subscribe else "Unsubscribed from"
        response = self.send_command(self.OPCODE_MAP['SUBSCRIBE_TO_CHAT'], payload)
        self.logger.info(f"{status} chat {chat_id}")
        if subscribe:
            self.subscribed_chats.add(str(chat_id))
        else:
            self.subscribed_chats.discard(str(chat_id))
        return response

    def mark_as_read(self, chat_id: str, message_id: str):
        payload = {"type": "READ_MESSAGE", "chatId": int(chat_id), "messageId": message_id, "mark": int(time.time() * 1000)}
        return self.send_command(self.OPCODE_MAP['MARK_AS_READ'], payload)

    def get_contact_details(self, contact_ids: list):
        payload = {"contactIds": [int(cid) for cid in contact_ids]}
        return self.send_command(self.OPCODE_MAP['GET_CONTACT_DETAILS'], payload)

    def get_contact_by_phone(self, phone_number: str):
        payload = {"phone": phone_number}
        return self.send_command(self.OPCODE_MAP['FIND_BY_PHONE_NUMBER'], payload)

    def get_chat_by_id(self, chat_id: str):
        return self.chats.get(chat_id)

    def get_all_chats(self):
        return self.chats

    def get_video(self, id: str):
        video_info = self.send_command(83, {"videoId": int(id), "token": self.token})
        video_info = video_info['payload']
        url = video_info.get('MP4_1080') or video_info.get('MP4_720')
        if not url:
            return None

        headers = {'User-Agent': self.user_agent['headerUserAgent']}

        with requests.get(url, headers=headers, stream=True, timeout=30) as r:
            r.raise_for_status()
            content_type = r.headers.get('content-type')
            if 'video' not in content_type:
                return None

            video_buffer = io.BytesIO()
            for chunk in r.iter_content(chunk_size=8192):
                video_buffer.write(chunk)

            video_buffer.seek(0)
            return video_buffer

    def get_file(self, id: str, chat_id: str, msg_id: str):
        file_info = self.send_command(88, {"fileId": int(id), "chatId": int(chat_id), "messageId": str(msg_id)})
        file_info = file_info['payload']
        url = file_info.get('url')
        if not url:
            return None

        with requests.get(url, timeout=30) as r:
            r.raise_for_status()
            file_content = r.content
            file_name = r.headers.get('X-File-Name') or "downloaded_file"

        return file_content, file_name
