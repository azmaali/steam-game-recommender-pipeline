import re
import tag_mappings  # make sure this is available or mock it for now

def normalize_tag_key(tag):
    if tag is None:
        return None
    tag = tag.lower().strip()
    tag = re.sub(r"[\s\-]+", " ", tag)
    tag = tag.replace(" ", "-")
    return tag

def normalize_tags(tags):
    if not tags:
        return []
    normalized = []
    for t in tags:
        if t != "" and t != "NA" and t is not None:
            key = normalize_tag_key(t)
            normalized.append(tag_mappings.tag_mappings.get(key, key))
    return normalized
