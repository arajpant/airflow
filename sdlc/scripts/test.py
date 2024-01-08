from cryptography.fernet import Fernet
import base64
import uuid

# Generate a valid Fernet key as UUID
secret_key = b'Rk91FBJeJyMNF3uPv5IWNaqrePqRHnSX3XJ4LDhwZDg='

# Pad text to be a multiple of 16 bytes
def pad_text(text):
    pad_size = 16 - len(text) % 16
    return text + pad_size * chr(pad_size)

# Unpad text by removing padding characters
def unpad_text(padded_text):
    pad_size = ord(padded_text[-1])
    return padded_text[:-pad_size]

# Encrypt plaintext using Fernet
def encrypt(plaintext, key):
    print("ENCRYPTION STARTED")
    print(f'secret key -> {key}')
    print(f'secret key length -> {len(key)}')
    f = Fernet(key)
    print('FERNET DECLARED')
    ciphertext = f.encrypt(pad_text(plaintext).encode())
    return base64.urlsafe_b64encode(ciphertext).decode()

# Decrypt ciphertext using Fernet
def decrypt(ciphertext, key):
    f = Fernet(key)
    ciphertext = base64.urlsafe_b64decode(ciphertext.encode())
    plaintext = f.decrypt(ciphertext).decode()
    return unpad_text(plaintext)

# Example usage
plaintext = "2048-01-15-8975"
ciphertext = encrypt(plaintext, secret_key)
print("Ciphertext:", ciphertext)
decrypted_text = decrypt(ciphertext, secret_key)
print("Decrypted Text:", decrypted_text)
# $m@rtBu$ine&$@54789