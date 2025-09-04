import random
from prefect import flow, task, get_run_logger


@task
def generate_a_number():
    return random.randint(0, 100)


@flow
def is_number_even(number: int):
    return number % 2 == 0


@flow
def even_or_odd():
    logger = get_run_logger()
    number = generate_a_number()
    if is_number_even(number):
        logger.info("The number is even")
    else:
        logger.info("The number is odd")


if __name__ == "__main__":
    even_or_odd()