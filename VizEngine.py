import subprocess
import generate_schedule_tex


class VizualizationEngine():
    def __init__(self, scalex, scaley, horizontal_ax_limit=0,
                 keep_intermediate_pdf=False):
        self.__scalex = scalex
        self.__scaley = scaley
        self.__limitx = horizontal_ax_limit
        self.__execution_log = []
        self.__keep_pdf = keep_intermediate_pdf

    def set_execution_log(self, execution_log):
        self.__execution_log = execution_log

    def set_horizontal_ax_limit(self, horizontal_ax_limit):
        self.__limitx = horizontal_ax_limit
        self.__scalex = 90/horizontal_ax_limit

    def generate_scenario_gif(self, name_scenario):
        makespan = self.__limitx
        scalex = self.__scalex
        if self.__limitx == 0:
            makespan = max([self.__execution_log[job]
                            [len(self.__execution_log[job]) - 1][1]
                            for job in self.__execution_log])*1./3600
            scalex = 90/makespan
        generate_schedule_tex.generate_animation_files(
            self.__execution_log, [job for job in self.__execution_log],
            (scalex, self.__scaley), name_scenario)
        subprocess.call(["./create_animation.sh",
                         name_scenario,
                         "delete"])
        return makespan
