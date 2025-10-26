from flask import Flask
import threading

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running!"

def main_bot_loop():


if __name__ == '__main__':
    threading.Thread(target=main_bot_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=10000)
