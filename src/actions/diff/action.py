def run(items_list: list) -> int:
    src_items = items_list[0]
    trg_items = items_list[1]
    src_rows_count = src_items[0][0] or 0
    trg_rows_count = trg_items[0][0] or 0
    return src_rows_count - trg_rows_count

