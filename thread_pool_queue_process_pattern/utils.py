def printProgressBar(
    iteration,
    total,
    prefix="Progress:",
    suffix="",
    decimals=1,
    length=50,
    fill="█",
    printEnd="\r",
):

    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + "-" * (length - filledLength)
    print(f"\r{prefix} |{bar}| {percent}% {suffix}", end=printEnd)
    if iteration == total:
        print(f"\r", end=printEnd)