import random
from faker import Faker
import datetime
import csv
import app_logger


class DataGenerator:

    def __init__(self, number_data=1000):
        self.number_data = number_data
        self.data = []
        self.faker = Faker()
        self.logger = app_logger.get_logger(__name__)

    def __return_name(self) -> str:
        """
        Generate random names
        Returns:
               str -> Random names
        """
        return self.faker.name()

    @staticmethod
    def __return_id() -> int:
        """
         Generate random Id 
         Returns:
                int -> four digit random Id
        """
        temp_id = [str(random.randint(1, 9))]
        for i in range(1, 5):
            temp_id.append(str(random.randint(0, 9)))
        return int("".join(temp_id))

    @staticmethod
    def __return_phone_number() -> int:
        """
         Generate random Phone numbers 
         Returns:
                int -> ten digit random phone number
        """
        temp_number = [str(random.randint(6, 9))]
        for i in range(1, 10):
            temp_number.append(str(random.randint(0, 9)))
        return int("".join(temp_number))

    def __return_address(self) -> str:
        """
         Generate random address
         Returns:
                str -> random address
        """
        cities = ['delhi', 'kolkata', 'bangalore', 'mysore', 'chandigarh', 'pune', 'chennai', 'hyderabad',
                  'ahmedabad', 'gurgaon', 'lucknow']
        return cities[random.randint(0, len(cities)-1)]

    @staticmethod
    def __return_pin() -> str:
        """
         Generate random pincode
         Returns:
                str -> seven digit random pincode
        """
        dirty_pin = random.randint(0, 1)
        temp_pin = [str(random.randint(1, 9))]
        for i in range(1, 7):
            temp_pin.append(str(random.randint(0, 9)))
        if dirty_pin:
            temp_pin[random.randint(0, 6)] = chr(random.randint(ord('a'), ord('z')))
        return "".join(temp_pin)

    @staticmethod
    def __return_email(name: str, id: int) -> str:
        """
         Generate random email_id by appending name,id and domain together
         Args:
             name(str) ---> Name of the user
             id(int)   ---> Id of the user
         Returns:
                str -> random email id
        """
        domain = ["gmail", "hotmail", "outlook"]
        name = name.replace(" ", "").lower()
        rnd_dmn = random.randint(0, len(domain)-1)
        email = "{0}_{1}@{2}.com".format(name, str(id), domain[rnd_dmn])
        return email

    @staticmethod
    def __return_dob() -> str:
        """
         Generate random Date of Birth
         Returns:
                str -> random date of birth
        """
        start_date = datetime.date(1986, 1, 1)
        end_date = datetime.date(2000, 2, 1)
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        random_date = start_date + datetime.timedelta(days=random_number_of_days)

        return str(random_date)

    def __save_data_to_file(self, filename):
        """
         Saving data to file 
         Args:
             filename(str) ---> Filename with complete path to save data to.
        """
        keys = self.data[0].keys()
        with open(filename, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(self.data)

    def generate_data(self, filename):
        """
        Generates Random Data for Given Amount and Writes to file
        Args:
             filename(str) ---> File with complete path to save data .

        Raises:
            Exception:
                Any issue with data creation or saving to file.
        """
        try:
            for i in range(0, self.number_data):
                dictionary_data = {'Name': self.__return_name(),
                                   'Id': self.__return_id(),
                                   'Phone_Number': self.__return_phone_number(),
                                   'Address': self.__return_address(),
                                   'Pin_Code': self.__return_pin(),
                                   'Email': None,
                                   'DOB': self.__return_dob()
                                   }

                dictionary_data["Email"] = self.__return_email(dictionary_data["Name"],
                                                               dictionary_data["Id"])

                self.data.append(dictionary_data)
                if i % 100 == 0:
                    self.logger.info("Generated %s rows.!" % str(i))

            self.__save_data_to_file(filename)

        except Exception as exp:
            self.logger.error(exp)
            raise Exception(exp)

    @property
    def get_keys(self):
        return self.data[0].keys()

    def data_type_key(self, key):
        if key in self.data[0].keys():
            return str(type(self.data[0][key])).split("'")[1]
        else:
            raise Exception("Given key does not exist!")

