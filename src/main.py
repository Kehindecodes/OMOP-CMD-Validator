from validator.validator import Validator


def main():
    validator = Validator("config/omop-schema-v5.4.yaml")
    validator.load_data("data")
    validator.validate_all()
    report = validator.get_report()

    print("\n--- Validation Report ---")
    if report["errors"]:
        print("Validation Failed with the following errors:")
        for error in report["errors"]:
            print(f"- {error}")
    else:
        print("Validation successful! No errors found.")


main()
