from mongodb import db_update_broker


def update_broker_info(topic, msg):
    db_update_broker(topic, msg)
    return None
