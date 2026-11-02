import os
import sys
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession
from settings_manager import load_settings, save_settings

async def main():
    print("=" * 60)
    print(" Telegram User Login (Full Channel History Mode)")
    print("=" * 60)
    print("This pairs your personal Telegram account so the addon can")
    print("scan 100% of your channel history (even before bot joined).\n")

    settings = load_settings()
    api_id = settings.get("api_id")
    api_hash = settings.get("api_hash")

    if not api_id or not api_hash:
        api_id_input = input("Enter your Telegram API ID: ").strip()
        api_hash_input = input("Enter your Telegram API Hash: ").strip()
        if not api_id_input or not api_hash_input:
            print("[Error] API ID and Hash are required.")
            return
        api_id = int(api_id_input)
        api_hash = api_hash_input

    phone = input("Enter your Telegram phone number (+country_code...): ").strip()
    if not phone:
        print("[Error] Phone number cannot be empty.")
        return

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()

    if not await client.is_user_authorized():
        await client.send_code_request(phone)
        code = input("Enter the 5-digit login code sent to your Telegram app: ").strip()
        try:
            await client.sign_in(phone, code)
        except Exception as e:
            if "password" in str(e).lower() or "two-step" in str(e).lower():
                pw = input("Enter your Telegram 2-Step Verification password: ").strip()
                await client.sign_in(password=pw)
            else:
                print(f"[Error] Sign-in failed: {e}")
                return

    session_str = client.session.save()
    me = await client.get_me()
    print(f"\n✅ Successfully authorized as: {me.first_name} (@{me.username or 'No username'})")

    settings["api_id"] = api_id
    settings["api_hash"] = api_hash
    settings["login_mode"] = "user"
    settings["session_string"] = session_str
    save_settings(settings)

    print("🎉 User session saved to settings.json!")
    print("Restart your Docker container to begin full historical scanning:")
    print("  docker compose restart\n")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
