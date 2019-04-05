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

        self.__generate_animation_files(name_scenario)
        subprocess.call(["./create_animation.sh",
                         name_scenario,
                         "delete"])
        return self.__limitx

    def __generate_animation_files(self, filename):
        ''' Generate a temp list of (start, end, procs,
        requested walltime, job_id, color) used to create
        the tex file '''
        run_list = []
        for job in self.__execution_log:
            run_list += self.__get_job_runs(self.__execution_log[job], job)
        run_list.sort()
        sliced_list = generate_schedule_tex.get_sliced_list(
            run_list)

        for i in range(len(run_list) + 1):
            outf = open(r'draw/%s_%d.tex' % (filename, i), 'w')
            # write header
            outf.writelines([l for l in
                             open("draw/tex_header").readlines()])
            generate_schedule_tex.print_execution_list(
                sliced_list, (self.__scalex, self.__scaley), i + 1, outf)
            if i < len(run_list):
                # write last job start and end times
                generate_schedule_tex.print_current_execution_info(
                    run_list[i], (self.__scalex, self.__scaley), outf)
            else:
                generate_schedule_tex.print_makespan(
                    max([r[1] for r in run_list]), self.__scalex, outf)
            # write footer
            outf.writelines([l for l in
                             open("draw/tex_footer").readlines()])
            outf.close()

    def __get_job_runs(self, execution_list, job):
        run_list = []
        requested_time = job.request_walltime
        for i in range(len(execution_list) - 1):
            # check failed executions
            start = execution_list[i][0]
            end = execution_list[i][1]
            run_list.append((start, end, job.nodes,
                             requested_time, job.job_id,
                             i + 1))
            # TODO - this will give an error if the sequence
            # list is not long enough
            if len(job.request_sequence) > i:
                requested_time = job.request_sequence[i]
            else:
                requested_time *= 1.5

        # check succesful execution (last run)
        start = execution_list[len(execution_list) - 1][0]
        end = execution_list[len(execution_list) - 1][1]
        run_list.append((start, end, job.nodes,
                         requested_time, job.job_id, 0))
        return run_list