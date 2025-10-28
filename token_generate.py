from fastapi import FastAPI, Form, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import requests
import os

app = FastAPI()

# Setup templates and static directories
os.makedirs("static", exist_ok=True)
os.makedirs("templates", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ----------------------------- SharekhanConnect Class -----------------------------
class SharekhanConnect:
    _rootUrl = "https://api.sharekhan.com/skapi"
    _routes = {
        'api.access.token': '/services/access/token',
        'api.login.url': '/auth/login.html'
    }
    
    def __init__(self):
        pass
    
    def login_url(self, api_key, vendor_key=None, version_id="1005", state="12345", callback_url=None):
        """Generate login URL for Sharekhan authentication"""
        base_url = f"{self._rootUrl}{self._routes['api.login.url']}"
        
        params = {
            'api_key': api_key,
            'state': state,
            'version_id': version_id
        }
        
        if callback_url:
            params['callback_url'] = callback_url
        if vendor_key:
            params['vendor_key'] = vendor_key
            
        # Build URL with parameters
        url_params = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{base_url}?{url_params}"
    
    def get_access_token(self, apiKey, encstr, state, vendorkey=None, versionId=None):
        """Generate access token using session token"""
        params = {
            'apiKey': apiKey,
            'requestToken': encstr,
            'state': state
        }
        
        if vendorkey is not None:
            params['vendorkey'] = vendorkey
        if versionId is not None:
            params['versionId'] = versionId
        
        response = self._postRequest('api.access.token', params)
        return response

    def _postRequest(self, route, params):
        url = f"{self._rootUrl}{self._routes[route]}"
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        response = requests.post(url, json=params, headers=headers)
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Sharekhan API Error: {response.text}"
            )
        
        try:
            return response.json()
        except ValueError:
            return {"response": response.text}

# Initialize SharekhanConnect
login = SharekhanConnect()

# ----------------------------- Routes -----------------------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("form.html", {"request": request})

@app.get("/login")
def login_route(app_id: str, vendor_key: str = None):
    """Redirect to Sharekhan login page"""
    version_id = "1005"
    state = "12345"
    callback_url = "http://127.0.0.1:8000/callback"
    
    login_url = login.login_url(
        api_key=app_id,
        vendor_key=vendor_key,
        version_id=version_id,
        state=state,
        callback_url=callback_url
    )
    
    return RedirectResponse(login_url)

@app.get("/callback", response_class=HTMLResponse)
def callback(request: Request, request_token: str = None, code: str = None, state: str = None):
    """Handle callback from Sharekhan after authentication"""
    print(f"REQUEST TOKEN: {request_token}")
    
    auth_code = request_token or code
    
    if not auth_code:
        return templates.TemplateResponse("result.html", {
            "request": request,
            "success": False,
            "error": "No authorization code received from Sharekhan."
        })
    
    return templates.TemplateResponse("form.html", {
        "request": request,
        "auth_code": auth_code,
        "message": "Authorization successful! Now enter your credentials to complete token generation."
    })

@app.post("/generate_token", response_class=HTMLResponse)
def generate_token(
    request: Request,
    app_id: str = Form(...),
    secret_id: str = Form(...),
    auth_code: str = Form(None),
    vendor_key: str = Form(None)):
    """Generate access token using the provided credentials"""
    if not auth_code:
        return RedirectResponse(url=f"/login?app_id={app_id}&vendor_key={vendor_key or ''}")
    
    try:
        sessionwithoutvesionId = login.generate_session_without_versionId(auth_code, secret_id)
        access_token = login.get_access_token(app_id, sessionwithoutvesionId, "12345")
        
        print("ACCESS TOKEN:", access_token)
        
        return templates.TemplateResponse("result.html", {
            "request": request,
            "success": True,
            "token_data": access_token
        })
    except Exception as e:
        return templates.TemplateResponse("result.html", {
            "request": request,
            "success": False,
            "error": f"Error: {str(e)}"
        })

# ----------------------------- Run -----------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)