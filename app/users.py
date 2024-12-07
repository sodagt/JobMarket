from pydantic import BaseModel
import json
import os

USER_DB = "users.json"

class UserSchema(BaseModel):
    username: str
    name: str
    password: str
    email: str
 #   acces: str


# Charger les utilisateurs depuis le fichier JSON
def load_users_from_file(file_path: str):
    with open(file_path, "r") as file:
        return json.load(file)
    

def save_users_to_file(file_path: str, users: dict):
    with open(file_path, "w") as file:
        json.dump(users, file, indent=4)



def read_users():
    if not os.path.exists(USER_DB):
        return []
    try:
        with open(USER_DB, "r") as file:
            data = file.read().strip()
            if not data:
                return []
            return json.loads(data)
    except json.JSONDecodeError:
        return []