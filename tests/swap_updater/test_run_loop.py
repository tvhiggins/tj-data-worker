from tj_worker import swap_updater


def test_run_loop():
    UpdateSwaps = swap_updater.SwapUpdater(testing=True)
    UpdateSwaps.run_loop()
