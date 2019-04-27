import subprocess
from distutils.spawn import find_executable


class TexGenerator():
    def __init__(self, execution_slices_list, execution_job_list,
                 scalex, scaley):
        self.__scalex = scalex
        self.__scaley = scaley
        self.__slices = execution_slices_list
        self.__run_list = execution_job_list
        self.__total_runs = len(execution_job_list)

    def write_to_file(self, filename):
        # create a file for each step of the simulation
        for i in range(self.__total_runs + 1):
            outf = open(r'draw/%s_%d.tex' % (filename, i), 'w')
            # write header
            outf.writelines([l for l in
                             open("draw/tex_header").readlines()])
            self.__print_execution_list(i + 1, outf)
            if i < self.__total_runs:
                # write last job start and end times
                self.__print_current_execution_info(self.__run_list[i],
                                                    outf)
            else:
                # last step in the simulation
                self.__print_makespan(max([r[1] for r in self.__run_list]),
                                      outf)
            # write footer
            outf.writelines([l for l in
                             open("draw/tex_footer").readlines()])
            outf.close()

    def __print_current_execution_info(self, execution, outf):
        start = float(execution[0])
        end = float(execution[1]) - start
        request = float(execution[3])
        outf.write(r'\legend{17}{-0.5}{Start:\ %.1f}' % (start))
        outf.write('\n')
        outf.write(r'\legend{40}{-0.5}{Duration:\ %.1f}' % (end))
        outf.write('\n')
        outf.write(r'\legend{65}{-0.5}{Request:\ %.1f}' % (request))
        outf.write('\n')

    def __print_makespan(self, value, outf):
        val = float(value)
        outf.write(r'\legend{%.1f}{-0.5}{%.1f}' % (val * self.__scalex, val))
        outf.write("\n")

    def __print_execution(self, execution, outf, last_frame):
        start = float(execution[0]) * self.__scalex
        end = float(execution[1]) * self.__scalex
        procs = execution[2] * self.__scaley
        offset = execution[6] * self.__scaley
        job_id = execution[4]
        color = 2 * min(execution[5], 5)
        color_text = r"{rgb:red,%d;yellow,%d}" % (
            color, 10 - color)
        if last_frame and color != 0:
            color_text = "white"
            job_id = ' '
        if start != end:
            # walltime box
            outf.write(r'''\draw[-, thick,fill=%s] (%.1f,%d)
                       rectangle node{$\scriptstyle{%s}$} (%.1f, %d);
                        ''' % (color_text, start, offset,
                               job_id, end, offset + procs))
            outf.write("\n")

    def __print_reservation(self, execution, outf):
        start = float(execution[0]) * self.__scalex
        procs = execution[2] * self.__scaley
        offset = execution[6] * self.__scaley
        request = float(execution[3]) * self.__scalex
        if start < request:
            # requested walltime box
            outf.write(r'''\draw[-, thick, dashed] (%.1f,%d)
                       rectangle (%.1f,%d) ;''' % (
                start, offset, request, offset + procs))
            outf.write("\n")

    def __print_execution_list(self, step, outf):
        # check if it is the last frame
        last_frame = False
        if step == len(self.__slices) + 1:
            last_frame = True
            step = step - 1
        for i in range(step):
            execution_list = self.__slices[i]
            # print all sliced of the current execution
            for execution in execution_list:
                self.__print_execution(execution, outf, last_frame)
            if not last_frame:
                self.__print_reservation(
                    execution_list[len(execution_list)-1], outf)


class VizualizationEngine():
    def __init__(self, procs, execution_log=[], horizontal_ax_limit=0,
                 keep_intermediate_pdf=False):
        self.__scaley = 150 / procs
        self.__limitx = horizontal_ax_limit
        self.__execution_log = execution_log
        self.__keep_pdf = keep_intermediate_pdf
        self.__set_scalex(execution_log)

        # check if pdflatex and convert from ImageMagik are installed
        assert (find_executable('pdflatex')), \
            'Pdflatex needs to be installed to create GIFs'
        assert (find_executable('convert')), \
            'Convert from ImageMagik needs to be installed to create GIFs'

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
        subprocess.call(["./draw/create_animation.sh",
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
        sliced_list = self.__get_sliced_list(run_list)

        tex_generator = TexGenerator(sliced_list, run_list,
                                     self.__scalex, self.__scaley)
        tex_generator.write_to_file(filename)

    def __find_running_jobs(self, run_list, start, end):
        return [i for i in range(len(run_list)) if
                run_list[i][0] <= start and
                run_list[i][1] >= end]

    def __get_sliced_list(self, run_list):
        ''' Generate a list of (start, end, procs, request_end,
        job_id, color, starty) '''
        event_list = list(set([i[0] for i in run_list] +
                              [i[1] for i in run_list]))
        event_list.sort()
        sliced_list = [[] for i in run_list]
        for i in range(len(event_list)-1):
            idx_list = self.__find_running_jobs(
                run_list, event_list[i], event_list[i + 1])
            idx_list.sort()
            starty = 0
            for idx in idx_list:
                sliced_list[idx].append(
                    (event_list[i], event_list[i + 1],
                     run_list[idx][2], run_list[idx][3] +
                     run_list[idx][0], run_list[idx][4],
                     run_list[idx][5], starty))
                starty += run_list[idx][2]
        return sliced_list

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
            requested_time = job.get_request_time(i + 1)

        # check succesful execution (last run)
        start = execution_list[len(execution_list) - 1][0]
        end = execution_list[len(execution_list) - 1][1]
        run_list.append((start, end, job.nodes,
                         requested_time, job.job_id, 0))
        return run_list
