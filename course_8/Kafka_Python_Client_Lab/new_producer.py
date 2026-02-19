from kafka import KafkaProducer
import json
producer = KafkaProducer(
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

session_open = True
while session_open:

    atm_id = input(
        "Provide ATM id (1 or 2). To close session, enter \"Q\":\n"
    )
    if atm_id.upper() == "Q":
        break
    elif atm_id in ("1", "2"):
        trans_id = input("Provide transaction id:\n")
        producer.send(
            "bankbranch_py",
            {'atmid':int(atm_id), 'transid':int(trans_id)}
        )
    else:
        print("The ATM id is not valid. The id must either 1 or 2.")
        continue

producer.flush()

producer.close()