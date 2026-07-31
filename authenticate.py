import time
import requests

def generate_and_save_github_token():
    request_headers = {
        "accept": "application/json",
        "editor-version": "Neovim/0.6.1",
        "editor-plugin-version": "copilot.vim/1.16.0",
        "content-type": "application/json",
        "user-agent": "GithubCopilot/1.155.0"
    }
    
    device_code_payload = '{"client_id":"Iv1.b507a08c87ecfe98","scope":"read:user"}'
    
    device_code_response = requests.post(
        "https://github.com/login/device/code",
        headers=request_headers,
        data=device_code_payload
    ).json()
    
    print(f"Go to: {device_code_response['verification_uri']}")
    print(f"Enter code: {device_code_response['user_code']}")
    
    while True:
        time.sleep(5)
        
        access_token_payload = f'{{"client_id":"Iv1.b507a08c87ecfe98","device_code":"{device_code_response["device_code"]}","grant_type":"urn:ietf:params:oauth:grant-type:device_code"}}'
        
        token_response = requests.post(
            "https://github.com/login/oauth/access_token",
            headers=request_headers,
            data=access_token_payload
        ).json()
        
        if "access_token" in token_response:
            github_token = token_response["access_token"]
            
            with open(".env", "w") as environment_file:
                environment_file.write(f"GITHUB_TOKEN={github_token}\n")
                
            print("Token successfully saved in .env file.")
            break

if __name__ == "__main__":
    generate_and_save_github_token()