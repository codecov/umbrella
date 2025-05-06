from shared.encryption.yaml_secret import yaml_secret_encryptor


def encode_secret_string(value) -> str:
    encryptor = yaml_secret_encryptor
    return f"secret:{encryptor.encode(value).decode()}"
