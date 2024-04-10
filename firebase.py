import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


def insert_into_firestore(values, collection: str):

    cred = credentials.Certificate("./service-account.json")

    app = firebase_admin.initialize_app(cred)

    db = firestore.client()

    for value in values:
        document = f"review_{value.review_id}"
        doc_ref = db.collection(collection).document(document)
        doc_ref.set(value.__dict__)

    print("Inserted into FireBase")
