class ProgressBar:
    def __init__(self, total):

        self.start_time = time.time()
        self.total = total

    def report(self, iteration):

        if iteration < self.total:
            self.print_progress_bar(iteration, self.total)
        else:
            self.print_progress_bar(self.total, self.total, suffix="Completed!")

    def print_progress_bar(self, iteration, total, suffix="", length=50, fill="█"):

        dur = time.time() - self.start_time
        time_str = time.strftime("%H:%M:%S", time.gmtime(dur))

        percent = ("{0:.1f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + "-" * (length - filledLength)
        print(f"\rProgress: |{bar}| {percent}% | {time_str}  {suffix}", end="\r")
        if iteration == total and suffix == "Completed!":
            print(f"\r", end="\n")