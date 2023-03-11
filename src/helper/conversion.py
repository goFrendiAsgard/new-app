def str_to_boolean(value: str):
    if value.lower() in ('0', 'false', 'no', 'n'):
        return False
    if value.lower() in ('1', 'true', 'yes', 'y'):
        return True
    raise Exception(f'Cannot convert to boolean: "{value}"')
