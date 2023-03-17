import base64


def _base_64_encode(input_json: str):
    doc_bytes = input_json.encode("utf-8")
    doc_encoded = base64.b64encode(doc_bytes)
    return str(doc_encoded, 'utf-8')
