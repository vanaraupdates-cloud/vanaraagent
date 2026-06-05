import os
import requests
from dotenv import load_dotenv

# Load configurations
load_dotenv()
token = os.getenv("LINKEDIN_ACCESS_TOKEN")

print("================================================================")
print("             LINKEDIN PERSON URN RETRIEVER")
print("================================================================")

if not token:
    print("\n[ERROR] No LINKEDIN_ACCESS_TOKEN found in your .env file.")
    print("Please make sure you have configured .env correctly.")
    exit(1)

print(f"Loaded token (first 10 chars): {token[:10]}...")

def try_userinfo():
    print("\nAttempting to fetch URN via OpenID Connect (/v2/userinfo)...")
    url = "https://api.linkedin.com/v2/userinfo"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            sub = data.get("sub")
            if sub:
                urn = f"urn:li:person:{sub}"
                print(f"[SUCCESS] Found Person URN: {urn}")
                print(f"Name: {data.get('given_name')} {data.get('family_name')}")
                return urn
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
    except Exception as e:
        print(f"Request failed: {e}")
    return None

def try_me():
    print("\nAttempting to fetch URN via Legacy Profile (/v2/me)...")
    url = "https://api.linkedin.com/v2/me"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            pid = data.get("id")
            if pid:
                urn = f"urn:li:person:{pid}"
                print(f"[SUCCESS] Found Person URN: {urn}")
                print(f"Name: {data.get('localizedFirstName')} {data.get('localizedLastName')}")
                return urn
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
    except Exception as e:
        print(f"Request failed: {e}")
    return None

# Try OIDC first
urn = try_userinfo()

# Fallback to legacy me if OIDC failed
if not urn:
    urn = try_me()

if urn:
    print("\n================================================================")
    print("                         ACTION REQUIRED")
    print("================================================================")
    print(f"Please update the following key in your .env file:")
    print(f"\nLINKEDIN_PERSON_URN={urn}")
    print("\n================================================================")
else:
    print("\n================================================================")
    print("                SCOPES / PERMISSIONS RESOLUTION")
    print("================================================================")
    print("Both endpoints returned ACCESS_DENIED (403).")
    print("Your current access token lacks the scopes to read your profile info.")
    print("To fix this, please follow these steps:")
    print("")
    print("1. Log in to the LinkedIn Developer Portal (https://developer.linkedin.com/)")
    print("2. Select your application.")
    print("3. Go to the 'Products' tab and ensure the following products are added:")
    print("   - 'Share on LinkedIn' (grants w_member_social)")
    print("   - 'Sign In with LinkedIn using OpenID Connect' (grants openid and profile)")
    print("4. Go to the 'Auth' tab and re-run your OAuth 2.0 flow to generate a new token.")
    print("   - When authorizing, make sure the scope parameter includes:")
    print("     openid profile w_member_social")
    print("5. Paste the new token into your .env file as LINKEDIN_ACCESS_TOKEN.")
    print("6. Run this script again: python get_my_urn.py")
    print("================================================================")
