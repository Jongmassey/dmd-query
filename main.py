from session import session
from query import opensafely_asthma_oral_prednisolone_medication


def main():
    s = session()
    q = opensafely_asthma_oral_prednisolone_medication(s)
    q.write.csv("opensafely-asthma-oral-prednisolone-medication.csv")


if __name__ == "__main__":
    main()
