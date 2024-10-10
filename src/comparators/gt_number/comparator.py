def run(**kwargs) -> bool:
    return kwargs["metric"] > float(kwargs["number"])
