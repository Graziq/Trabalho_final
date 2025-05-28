from prefect import flow

@flow
def hello_flow():
    print("Hello, World!")

if __name__ == "__main__":
    hello_flow()
