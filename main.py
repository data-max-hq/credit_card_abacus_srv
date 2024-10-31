from credit_card_abacus_service import CreditCardAbacusService

if __name__ == "__main__":
    service = CreditCardAbacusService()

    # Start the service, execute the tasks, and finish
    print("Running CreditCardAbacusService...")
    service.run()
    print("CreditCardAbacusService completed.")