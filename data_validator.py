import csv
import os


class DataValidator:
    def __init__(self, filename):
        self.filename = filename
        self.complete_data = []
        self.keys = []
        self.error_index = set()
        self.error_data = []
        self.final_data = []

    def read_file_data(self):
        with open(self.filename) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                self.complete_data.append(row)
        self.keys = self.complete_data[0]
        self.complete_data = self.complete_data[1:]

    def validate_numerical_data(self, index):
        for ind in range(0, len(self.complete_data)):
            if not self.complete_data[ind][index].isnumeric():
                self.error_index.add(ind)

    def validate_email(self, index):
        self.validate_space(index)
        for ind in range(0, len(self.complete_data)):
            if "@" not in self.complete_data[ind][index]:
                self.error_index.add(ind)

    def validate_space(self, index):
        for ind in range(0, len(self.complete_data)):
            if " " in str(self.complete_data[ind][index]):
                self.error_index.add(ind)

    def finalize_data(self):
        for ind in range(0, len(self.complete_data)):
            if ind in self.error_index:
                self.error_data.append(self.complete_data[ind])
            else:
                self.final_data.append(self.complete_data[ind])

    def write_validated_data(self):
        final_data_file = os.path.join(os.path.dirname(self.filename), "final_data.csv")
        error_data_file = os.path.join(os.path.dirname(self.filename), "error_data.csv")
        with open(final_data_file, 'w', newline='') as output_file:
            writer = csv.writer(output_file, delimiter=",")
            writer.writerows([self.keys])
            writer.writerows(self.final_data)
        with open(error_data_file, 'w', newline='') as output_file:
            writer = csv.writer(output_file, delimiter=",")
            writer.writerows([self.keys])
            writer.writerows(self.error_data)
        return (final_data_file, error_data_file)

    def validate_data_main(self):
        self.read_file_data()
        # 1. Validating Number for Pin code
        self.validate_numerical_data(self.keys.index("Pin_Code"))
        # 2. Validating Number for Phone Number
        self.validate_numerical_data(self.keys.index("Phone_Number"))
        # 3. Validating email:
        self.validate_email(self.keys.index("Email"))
        # 4. Validate Space in Number, Pin_Code, Id:
        self.validate_space(self.keys.index("Phone_Number"))
        self.validate_space(self.keys.index("Pin_Code"))
        self.validate_space(self.keys.index("Id"))

        self.finalize_data()
        final_data_file, _ = self.write_validated_data()
        return final_data_file



