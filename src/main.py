from validator.validator import Validator
import time

def main():
    validator = Validator("config/omop-schema-v5.4.yaml")
    validator.load_data("data")
    start_time = time.time()
    validator.validate_all()
    print(f"Validation completed in {round(time.time() - start_time, 2)} seconds")
    report = validator.get_report()

    print("\n--- Validation Report ---")
    if report["errors"]:
        print("Validation Failed with the following errors:")
        for error in report["errors"]:
            print(f"- {error}")
    else:
        print("Validation successful! No errors found.")


main()
