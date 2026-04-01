import time
import os
from flask import Flask, request, jsonify
from threading import Thread
from functions.bot import Bot
from functions.helpers import log

import json

from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

bot = Bot()
bot_thread = None

import pathlib

@app.route('/', methods=['POST'])
def handle_request():
    global bot, bot_thread

    data = request.get_json()  # Get the JSON data from the request
    function = data.get('function')  # Extract the 'function' field
    from_restarter = data.get('from_restarter')

    if function == 'start':
        if not from_restarter:
            with open("BOT_STARTED", "w") as file:
                file.write("")
        probability_threshold = data.get('probability_threshold')
        tradable_percentage = data.get('tradable_percentage')
        number_of_retries = data.get('number_of_retries')
        log(f"probability_threshold: {probability_threshold}")
        log(f"tradable_percentage: {tradable_percentage}")
        log(f"number_of_retries: {number_of_retries}")

        if bot.is_running:
            return jsonify({"error": "Bot is already running"}), 400

        try:
            bot.stop()
            bot_thread.join()
            print('Bot Stopped')
        except:
            log('exception while stopping the bot before being started')

        def run_bot():
            log("[SERVER] Starting bot thread...")
            log(f"[SERVER] Bot parameters: threshold={probability_threshold}, tradable={tradable_percentage}%, retries={number_of_retries}")
            try:
                bot.start(probability_threshold, tradable_percentage, number_of_retries)
                log("[SERVER] Bot started successfully")
            except Exception as e:
                log(f"[SERVER] Bot startup failed: {type(e).__name__}: {e}")
                print(e)
                import traceback
                log(f"[SERVER] Traceback: {traceback.format_exc()}")
                raise

        log("[SERVER] Creating bot thread...")
        bot_thread = Thread(target=run_bot)
        bot_thread.daemon = False  # prevent process from exiting if thread is running
        bot_thread.start()
        time.sleep(4)
        log("[SERVER] Bot thread started, waiting for initialization...")
        print('Bot Started')

        # bot.start()
        # print('Bot Started')
    elif function == 'stop':
        if not from_restarter:
            if os.path.exists("BOT_STARTED"):
                os.remove("BOT_STARTED")

        bot.stop()
        bot_thread.join()  # Wait for the bot thread to finish
        time.sleep(2)
        print('Bot Stopped')
    elif function == 'get_state':
        pass  # Just return the current state
    else:
        return jsonify({"error": "Invalid function"}), 400

    return jsonify({"bot_is_running": bot.is_running})  # Return the current state

@app.get("/health")
def health():
    return {"ok": True}

if __name__ == '__main__':
    
    # needed for railway deployment
    
    for cred_path in ["./secrets/client_credentials.json", "./secrets/my_credentials.json"]:
        path = pathlib.Path(cred_path)
        
        if not path.exists():
            raw = os.getenv("CREDENTIALS")
            if raw:
                creds = json.loads(raw)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(creds, indent=4))
                
    port = int(os.getenv("PORT", 8000))
    print(f"[SETUP] Discovered port: {port}")     
    timeframe = os.getenv("TIMEFRAME", "1h")
    print(f"Using timeframe: {timeframe}")
    
    use_trailing = bool(os.getenv("USE_TRAILING"))
    print(f"Using trailing? {use_trailing}")
    
    rr = float(os.getenv("RISK_REWARD", 2.0))
    print(f"Using risk-reward ratio: {rr}")
    
    decision_threshold = float(os.getenv("DECISION_THRESHOLD", 0.65))
    print(f"Using decision threshold: {decision_threshold}")
    
    strategy_name = os.getenv("STRATEGY_NAME")
    print(f"Using strategy: {strategy_name}")
    
    include_spread = bool(os.getenv("INCLUDE_SPREAD", True))
    print(f"Include spread: {include_spread}")

    use_model = bool(os.getenv("USE_MODEL", True))
    print(f"Use model: {use_model}")
    
    use_price_vector = bool(os.getenv("USE_PRICE_VECTOR", True))
    print(f"Use price vector: {use_price_vector}")
        
    soft_threshold = float(os.getenv("SOFT_THRESHOLD", 0.005))
    hard_threshold = float(os.getenv("HARD_THRESHOLD", 0.01))
    print(f"Soft threshold: {soft_threshold}, hard threshold: {hard_threshold}")
    
    app.run(host="0.0.0.0", port=port)
