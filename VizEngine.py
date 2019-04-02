import subprocess
import generate_schedule_tex


class VizualizationEngine():
    def __init__(self, procs, execution_log=[], horizontal_ax_limit=0,
                 keep_intermediate_pdf=False):
        self.__scaley = 150 / procs
        self.__limitx = horizontal_ax_limit
        self.__execution_log = execution_log
        self.__keep_pdf = keep_intermediate_pdf
        self.__set_scalex(execution_log)

    def __set_scalex(self, execution_log):
        if len(execution_log) > 0:
            limitx = max([execution_log[job][len(execution_log[job]) - 1][1]
                          for job in execution_log])
            if limitx > self.__limitx:
                self.__limitx = limitx
            self.__scalex = 90 / self.__limitx

    def set_execution_log(self, execution_log):
        self.__execution_log = execution_log 
        self.__set_scalex(execution_log)

    def set_horizontal_ax_limit(self, horizontal_ax_limit):
        self.__limitx = horizontal_ax_limit
        self.__scalex = 90/horizontal_ax_limit

    def generate_scenario_gif(self, name_scenario):
        assert (len(self.__execution_log) > 0),\
            'ERR - Trying to create an animation for an empty execution log'

        generate_schedule_tex.generate_animation_files(
            self.__execution_log, [job for job in self.__execution_log],
            (self.__scalex, self.__scaley), name_scenario)
        subprocess.call(["./create_animation.sh",
                         name_scenario,
                         "delete"])
        return self.__limitx

