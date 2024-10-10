def run(items_list: list) -> float:
    items = items_list[0]
    current_count = float(items[0][0]) or 0.0
    previous_count = float(items[0][1]) or 0.0
    if previous_count:
        return current_count / previous_count
    return -1
