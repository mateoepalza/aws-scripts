

DYC = {
    " ": 0,
    "a": 1,
    "b": 2,
    "c": 3,
    "d": 4,
    "e": 5,
    "f": 6,
    "g": 7,
    "h": 8,
    "i": 9,
    "j": 10,
    "k": 11,
    "l": 12,
    "m": 13,
    "n": 14,
    "ñ": 15,
    "o": 16,
    "p": 17,
    "q": 18,
    "r": 19,
    "s": 20,
    "t": 21,
    "u": 22,
    "v": 23,
    "x": 24,
    "y": 25,
    "z": 26
}

MSJ = "oiga carlos despidase"

def encrypt():
    msgEncryoted = []
    for i in MSJ:
        p = pow(DYC[i], 9)
        e = int(p) % 46
        msgEncryoted.append(e)

    return msgEncryoted

def decrypt(msgEncrypted):
    msgDecrypted = []
    for char in msgEncrypted:
        p = pow(char, 5)
        e = int(p) % 46
        msgDecrypted.append(e)

    return msgDecrypted

if "__main__" == __name__: 
    msgEncrypted = encrypt()
    msgDecrypted = decrypt(msgEncrypted)

    print("MSG encrypted: ", msgEncrypted)
    print("MSG dencrypted: ", msgDecrypted)