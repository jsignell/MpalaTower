'''
Parse campbell sci programs and glean information on variables

Julia Signell
2015-05-19
'''

program = 'C:/Users/Julia/Dropbox (PE)/KenyaLab/Data/Tower/TowerData/raw_netCDF_output/programs/RadioTowerCR216_soil_V2.CR2'
datafile = 'soil'
serial = 'SN22030'

prog = open(program)
program_content = prog.readlines()
prog.close()


def str_to_dict(s, make_guess=False):
    '''Create dict from string containing dict and optionally an = sign.'''
    from ast import literal_eval
    import warnings
    if '{' not in s:
        warnings.warn("no dict found in line: '%s'" % s, SyntaxWarning)
        s_dict = {}  
    else:
        dict_str = s[s.find("{"):s.find("}")+1]  # take only what's in {}
        s_dict = literal_eval(dict_str)  # evaluate string as dict
        s = s.strip(dict_str)  # take out the dict from the string
    if make_guess is True:
        elements = s.split()  # separate string into list using space
        if '=' not in elements:
            i = 0
            for element in elements:
                if '=' in element:
                    key = element.partition('=')[0]
                    val = element.partition('=')[2]
                    s_dict.update({key: val})
                    i+=1
            if i == 0:
                warnings.warn("no '=' found in line: '%s'" % s, SyntaxWarning)
        else:
            # find elements on either side of the equals sign
            eq = elements.index('=')
            val = elements.pop(eq+1)
            key = elements.pop(eq-1)
            s_dict.update({key: val})
    return s_dict


def get_programmed_coords(program_content=None, serial=None):
    lines = program_content
    for line in lines:
        if "'site'" in line and serial in line:
            coords = str_to_dict(line)
    return coords


def get_programmed_units_info(program_content=None):
    lines = program_content
    csi_info = []
    sensors = []
    for line in lines:
        if line.startswith('Units'):
            csi_info.append(str_to_dict(line, make_guess=True))
        if "'model'" in line:
            sensors.append(str_to_dict(line))
    for u in csi_info:
        k = [k for k in u.keys() if k != 'limits'][0]
        val = u.pop(k)
        u.update({'public': k, 'units': val})
        for line in lines:
            if 'Alias ' in line and k in line:
                public = str_to_dict(line, make_guess=True).keys()[0]
                if public.startswith(k):
                    if 'aliases' not in u.keys():
                        u.update({'aliases': []})
                    alias = str_to_dict(line, make_guess=True).pop(public)
                    u['aliases'].append(alias)
            if 'Dim ' in line and k in line.split()[1]:
                comment = line.partition("'")[2].strip('.\n')  # we want after the '
                if 2<len(comment):
                    u.update({'description': comment})
        for sensor in sensors:
            for var in sensor['variables']:
                if k.startswith(var):
                    u.update(sensor)
                    u.pop('variables')
    return csi_info


def convert_to_sec(num, units):
    if units.startswith(('Min', 'min')):
        out = int(num) * 60
    elif units.startswith(('ms', 'mS')):
        out = float(num) / 1000
    elif units.startswith(('s', 'S')):
        out = int(num)
    else:
        print('couldn\'t parse units')
        return (num, units)
    return out


def get_programmed_frequency(program_content=None, datafile=None):

    lines = program_content
    i = 0
    k = 0
    interval = None
    dt = 'DataTable'
    di = 'DataInterval'
    ct = 'CallTable'
    for i in range(len(lines)):
        line = lines[i].lstrip()
        if line.startswith(dt) and datafile in line:
            k = i
        if line.startswith(di) and i <= (k + 2):
            interval = line.split(',')[1]
            units = line.split(',')[2]
        i += 1
    if interval is None:
        i = 0
        for i in range(len(lines)):
            line = lines[i].lstrip()
            if line.startswith('Scan'):
                interval_temp = line.split('(')[1].split(',')[0]
                units_temp = line.split(',')[1]
                k = i
            if line.startswith(ct) and datafile in line and i <= (k + 7):              
                interval = interval_temp
                units = units_temp
            i += 1
    if interval is None:
        frequency_flag = 'could not find interval in %s' % program
        frequency = float('nan')
        timestep_count = int(0)
        return [frequency, frequency_flag, timestep_count]
    try:
        num = int(interval)
    except:
        for l in lines:
            line = l.lstrip()
            if line.startswith('Const ' + interval):
                a = line.split('=')[1]
                b = a.split()[0]
                num = int(b)
    frequency = convert_to_sec(num, units)
    timestep_count = int(24. * 60. * 60. / frequency)
    frequency_flag = 'found frequency'
    return [frequency, frequency_flag, timestep_count]
