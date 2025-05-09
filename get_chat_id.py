import requests
import json

BOT_TOKEN = "8045818201:AAGaU25sYspHX7M6oX7u-tWC7o5XgJ2MfCs"

url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
res = requests.get(url).json()

print(json.dumps(res, indent=2))

for update in res["result"]:
    message = update.get("message", {})
    chat = message.get("chat", {})
    print("Chat ID:", chat.get("id"))
    print("First Name:", chat.get("first_name"))
    print("Message:", message.get("text"))
