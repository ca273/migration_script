import csv


# Function to generate unique first and last names
def generate_unique_names(first_name, last_name, index):
    return f"{first_name}_{index}", f"{last_name}_{index}"


# Function to generate CSV data and write to file
def generate_csv(filename, num_rows):
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['first name', 'last name', 'address', 'age'])

        for i in range(1, num_rows + 1):
            first_name, last_name = generate_unique_names("mohammad", "ahmad", i)
            address = "amman"
            age = 20
            writer.writerow([first_name, last_name, address, age])

    print(f"CSV file '{filename}' generated successfully with {num_rows} rows.")


# Define parameters
csv_filename = "data_to_migrate/users.csv"
num_rows = 20_000_000  # Number of rows to generate

# Generate the CSV file
generate_csv(csv_filename, num_rows)
