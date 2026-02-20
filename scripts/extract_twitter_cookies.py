"""Extract Twitter/X cookies by connecting to the user's running Chrome.

Usage: Close Chrome, then run this script. It will launch Chrome with
your existing profile, let you verify you're logged in, then extract
auth_token and ct0 cookies automatically.
"""

import asyncio
import json
import sys
from pathlib import Path

async def main():
    from playwright.async_api import async_playwright

    print("Launching Chrome with your profile to extract x.com cookies...")
    print("(A Chrome window will open briefly — don't close it)\n")

    async with async_playwright() as p:
        # Launch Chromium and navigate to x.com
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # We can't use the user's Chrome profile directly in headless mode
        # when Chrome is running. Instead, let's try CDP connection.
        await browser.close()

        # Try connecting to running Chrome via CDP
        # First check if Chrome has debugging port open
        try:
            browser = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
            contexts = browser.contexts
            if contexts:
                cookies = await contexts[0].cookies(["https://x.com", "https://twitter.com"])
                auth_token = next((c["value"] for c in cookies if c["name"] == "auth_token"), None)
                ct0 = next((c["value"] for c in cookies if c["name"] == "ct0"), None)
                if auth_token and ct0:
                    print(f"SUCCESS! Extracted cookies from running Chrome.")
                    print(f"auth_token={auth_token}")
                    print(f"ct0={ct0}")
                    await browser.close()
                    return auth_token, ct0
            await browser.close()
        except Exception:
            pass

        # Fallback: launch with user's Chrome profile
        # This requires Chrome to be closed
        import os
        user_data = os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google", "Chrome", "User Data")

        try:
            context = await p.chromium.launch_persistent_context(
                user_data_dir=user_data,
                headless=False,
                channel="chrome",
                args=["--profile-directory=Default"],
            )

            page = await context.new_page()
            await page.goto("https://x.com/home", wait_until="networkidle", timeout=30000)
            await asyncio.sleep(3)

            cookies = await context.cookies(["https://x.com", "https://twitter.com"])
            auth_token = next((c["value"] for c in cookies if c["name"] == "auth_token"), None)
            ct0 = next((c["value"] for c in cookies if c["name"] == "ct0"), None)

            await context.close()

            if auth_token and ct0:
                print(f"SUCCESS!")
                print(f"auth_token={auth_token}")
                print(f"ct0={ct0}")
                return auth_token, ct0
            else:
                print("ERROR: Not logged in to x.com — no auth cookies found")
                return None, None

        except Exception as e:
            if "already in use" in str(e).lower() or "lock" in str(e).lower():
                print("ERROR: Chrome is running. Please close Chrome first, then re-run.")
            else:
                print(f"ERROR: {e}")
            return None, None


if __name__ == "__main__":
    result = asyncio.run(main())
    if result and result[0] and result[1]:
        # Write to config
        config_path = Path(__file__).resolve().parent.parent / "configs" / "default.yaml"
        print(f"\nTo use these cookies, update {config_path}")
        print(f'  auth_token: "{result[0]}"')
        print(f'  ct0: "{result[1]}"')
