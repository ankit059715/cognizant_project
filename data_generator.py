import random
from faker import Faker
import datetime
import csv


class DataGenerator:

    def __init__(self, number_data=1000):
        self.number_data = number_data
        self.data = []
        self.faker = Faker()

    def __return_name(self) -> str:
        return self.faker.name()

    @staticmethod
    def __return_id() -> int:
        temp_id = [str(random.randint(1, 9))]
        for i in range(1, 5):
            temp_id.append(str(random.randint(0, 9)))
        return int("".join(temp_id))

    @staticmethod
    def __return_phone_number() -> int:
        temp_number = [str(random.randint(6, 9))]
        for i in range(1, 10):
            temp_number.append(str(random.randint(0, 9)))
        return int("".join(temp_number))

    def __return_address(self) -> str:
        return self.faker.address()

    @staticmethod
    def __return_pin() -> str:
        dirty_pin = random.randint(0, 1)
        temp_pin = [str(random.randint(1, 9))]
        for i in range(1, 7):
            temp_pin.append(str(random.randint(0, 9)))
        if dirty_pin:
            temp_pin[random.randint(0, 6)] = chr(random.randint(ord('a'), ord('z')))
        return "".join(temp_pin)

    @staticmethod
    def __return_email(name: str, id: int) -> str:
        domain = ["gmail", "hotmail", "outlook"]
        name = name.replace(" ", "").lower()
        rnd_dmn = random.randint(0, len(domain)-1)
        email = "{0}_{1}@{2}.com".format(name, str(id), domain[rnd_dmn])
        return email

    @staticmethod
    def __return_dob() -> str:
        start_date = datetime.date(1986, 1, 1)
        end_date = datetime.date(2000, 2, 1)
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        random_date = start_date + datetime.timedelta(days=random_number_of_days)

        return str(random_date)

    def __save_data_to_file(self, filename):
        keys = self.data[0].keys()
        with open(filename, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.data)

    def generate_data(self, filename="project_data.csv"):
        """
        Generates Random Data for Given Amount and Writes to file
        Args:
             filename(str) ---> File to save data to.

        Raises:
            Exception:
                Any issue with data creation or saving to file.
        """
        try:
            for i in range(0, self.number_data):
                dictionary_data = {'Name': self.__return_name(),
                                   'Id': self.__return_id(),
                                   'Phone Number': self.__return_phone_number(),
                                   'Address': self.__return_address(),
                                   'Pin Code': self.__return_pin(),
                                   'Email': None,
                                   'Date of Birth': self.__return_dob()
                                   }

                dictionary_data["Email"] = self.__return_email(dictionary_data["Name"],
                                                               dictionary_data["Id"])

                self.data.append(dictionary_data)

            self.__save_data_to_file(filename)

        except Exception as exp:
            raise Exception(exp)


if __name__ == "__main__":
    data_generator = DataGenerator()
    data_generator.generate_data()

