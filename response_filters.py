# A known cache/origin error body used HTTP 200. Match a specific combination
# of phrases rather than rejecting legitimate articles about access control.
def is_access_block(text):
    lowered = text.casefold()
    return (
        "access restricted - ip location block" in lowered
        and "website owner has configured geographic access restrictions" in lowered
    )
