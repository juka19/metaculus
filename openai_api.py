import openai
import pandas as pd

with open("assets/api_key.txt", "r") as f:
    openai.api_key = f.read()

def ask_metaculus_question(q):
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {
                "role": "user",
                "content": """
                In the following, I will ask you a number of questions. The questions contain a certain degree of uncertainty, because their answer is unknown. I want you to answer the questions shortly and concisely, directly addressing the question and would like you to give your best guess at all times. If the question is binary (true/false), I would like you to answer with true or false only. If the question asks for your guess on a specific date, I would like you to answer with a date in YYYY/MM/DD format only. If the questions ask for a likelihood, I would like you to answer with a probability between 0% and 100% only. If the question pertains a price, I want you to answer with the number of the price, in the specified currency only. I would like you to name an expert for the topic of the respective question and answer as if you were him/her. Print your answer like this: "Name of the identified expert: Probability: X%"
                """
            },
            {
                "role": "assistant",
                "content": "Understood! I'll do my best to answer your questions with concise and direct responses, providing my best guess within the given parameters. Please go ahead and ask your questions."
            },
            {
                "role": "user",
                "content": q
            }
        ]
    )
    chat_response = completion.choices[0].message.content
    return chat_response


data = pd.read_csv("metaculus_sep2021.csv")

ask_metaculus_question(data["title"][0])