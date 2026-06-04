"""ILAM Portal — Router Auth"""
from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from datetime import datetime, timedelta
from app.config import settings
from app.models.schemas import LoginRequest, TokenResponse, UserRole

router = APIRouter(prefix="/api/auth", tags=["auth"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# Utilisateurs en dur (remplacé par Keycloak en production)
USERS = {
    "ilam_admin": {
        "password": "ilam_password_2025",
        "role"    : UserRole.ADMIN,
        "email"   : "admin@ilam.io",
    },
    "engineer": {
        "password": "engineer_2025",
        "role"    : UserRole.ENGINEER,
        "email"   : "engineer@sonatel.sn",
    },
    "analyst": {
        "password": "analyst_2025",
        "role"    : UserRole.ANALYST,
        "email"   : "analyst@sonatel.sn",
    },
    "viewer": {
        "password": "viewer_2025",
        "role"    : UserRole.VIEWER,
        "email"   : "viewer@sonatel.sn",
    },
}

def create_token(username: str, role: str) -> str:
    expire = datetime.utcnow() + timedelta(
        minutes=settings.TOKEN_EXPIRE)
    return jwt.encode(
        {"sub": username, "role": role, "exp": expire},
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM,
    )

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload  = jwt.decode(
            token, settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM])
        username = payload.get("sub")
        role     = payload.get("role")
        if not username:
            raise HTTPException(status_code=401)
        return {"username": username, "role": role}
    except JWTError:
        raise HTTPException(
            status_code=401,
            detail="Token invalide ou expiré")

@router.post("/login", response_model=TokenResponse)
def login(request: LoginRequest):
    user = USERS.get(request.username)
    if not user or user["password"] != request.password:
        raise HTTPException(
            status_code=401,
            detail="Identifiants incorrects")
    token = create_token(request.username, user["role"])
    return TokenResponse(
        access_token=token,
        role=user["role"],
        username=request.username,
    )

@router.get("/me")
def get_me(current_user=Depends(get_current_user)):
    return current_user
