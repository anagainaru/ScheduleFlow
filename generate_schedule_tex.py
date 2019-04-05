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
    start = float(execution[0]) * scale[0]
    end = float(execution[1]) * scale[0]
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
    start = float(execution[0]) * scale[0]
    procs = execution[2] * scale[1]
    offset = execution[6] * scale[1]
    request = float(execution[3]) * scale[0]
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
    start = float(execution[0])
    request = start + float(execution[3])
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
    val = float(value)
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
