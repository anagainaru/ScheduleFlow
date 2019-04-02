def get_job_runs(execution_list, job):
    run_list = []
    requested_time = job.request_walltime
    for i in range(len(execution_list) - 1):
        # check failed executions
        start = execution_list[i][0]
        end = execution_list[i][1]
        run_list.append((start, end, job.nodes,
                         requested_time, job.job_id,
                         i + 1))
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


def print_execution(execution, scale, outf, last_frame):
    start = float(execution[0] / 3600) * scale[0]
    end = float(execution[1] / 3600) * scale[0]
    procs = execution[2] * scale[1]
    offset = execution[6] * scale[1]
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


def print_reservation(execution, scale, outf):
    start = float(execution[0] / 3600) * scale[0]
    procs = execution[2] * scale[1]
    offset = execution[6] * scale[1]
    request = float(execution[3] / 3600) * scale[0]
    if start < request:
        # requested walltime box
        outf.write(r'''\draw[-, thick, dashed] (%.1f,%d)
                   rectangle (%.1f,%d) ;''' % (
            start, offset, request, offset + procs))
        outf.write("\n")


def print_execution_list(sliced_list, scale, step, outf):
    # check if this is the last frame
    last_frame = False
    if step == len(sliced_list) + 1:
        last_frame = True
        step = step - 1
    for i in range(step):
        execution_list = sliced_list[i]
        # print all sliced of the current execution
        for execution in execution_list:
            print_execution(execution, scale, outf, last_frame)
        if not last_frame:
            print_reservation(
                execution_list[len(execution_list)-1],
                scale, outf)


def print_current_execution_info(execution, scale, outf):
    start = float(execution[0] / 3600)
    request = start + float(execution[3] / 3600)
    plot_end = request * scale[0]
    if request-start < 15:
        plot_end += 5
    if start > 0.7:
        outf.write(r'\xlegend{%.1f}{%.1f}' % (start * scale[0],
                                              start))
        outf.write("\n")
    outf.write(r'\xlegend{%.1f}{%.1f}' % (plot_end, request))
    outf.write("\n")


def print_makespan(value, scale, outf):
    val = float(value / 3600)
    outf.write(r'\xlegend{%.1f}{%.1f}' % (val * scale, val))
    outf.write("\n")


def find_running_jobs(run_list, start, end):
    return [i for i in range(len(run_list)) if
            run_list[i][0] <= start and
            run_list[i][1] >= end]


def get_sliced_list(run_list):
    ''' Generate a list of (start, end, procs, request_end,
    job_id, color, starty) '''
    event_list = list(set([i[0] for i in run_list] +
                          [i[1] for i in run_list]))
    event_list.sort()
    sliced_list = [[] for i in run_list]
    for i in range(len(event_list)-1):
        idx_list = find_running_jobs(run_list, event_list[i],
                                     event_list[i + 1])
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


def generate_animation_files(workload, job_list, scale, filename):
    ''' Generate a temp list of (start, end, procs,
    requested walltime, job_id, color) used to create
    the tex file '''
    run_list = []
    for job in workload:
        job_info = [j for j in job_list if j.job_id == job.job_id]
        if len(job_info) != 1:
            print(r"ERR inexisting job in the list %s .. \
                  cannot continue" % (job))
            return []
        run_list += get_job_runs(workload[job], job_info[0])
    run_list.sort()
    sliced_list = get_sliced_list(run_list)

    for i in range(len(run_list) + 1):
        outf = open(r'draw/%s_%d.tex' % (filename, i), 'w')
        # write header
        outf.writelines([l for l in
                         open("draw/tex_header").readlines()])
        print_execution_list(sliced_list, (scale[0], scale[1]),
                             i + 1, outf)
        if i < len(run_list):
            # write last job start and end times
            print_current_execution_info(
                run_list[i], (scale[0], scale[1]), outf)
        else:
            print_makespan(max([r[1] for r in run_list]),
                           scale[0], outf)
        # write footer
        outf.writelines([l for l in
                         open("draw/tex_footer").readlines()])
        outf.close()
