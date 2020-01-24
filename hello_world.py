import faust
from stringLengthFunc import length
import random

app=faust.App(
    'hello-world',
    broker='kafka://localhost:9092',
    value_serializer='raw',
    )

greetings_topic=app.topic('greetings')

@app.agent(greetings_topic)
async def greet(greetings):
    async for greeting in greetings:
        strlen=length.stringLength(greeting)
        entry={'upper':str(greeting).upper(), 'str_length': strlen}
        print(entry)
        return entry

@app.agent()
async def say(greetings):
    async for greeting in greetings:
        print(greeting)

@app.timer(2.0, on_leader=True)
async def publish_greetings():
    print('PUBLISH ON LEADER!')
    greeting=str(random.random())
    await say.send(value=greeting)