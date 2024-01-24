from tj_worker import swap_getter


def test_run_loop(test_container_name):

    GetSwaps = swap_getter.SwapGetter(testing=True, azure_storage_container=test_container_name)
    GetSwaps.run_loop()
    GetSwaps.clear_existing_files()
