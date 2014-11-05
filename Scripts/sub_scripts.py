import csv
import sys

def fill_zero_length(fields):
    for e in range(len(fields)):
        if fields[e] == 0:
            fields[e] = 10
    return fields

def get_group(line):
    field = line.rfind('|') + 2
    return field

def pie_dedupe(input_file,out_file):
    print 'Creating Pie database upload stat file'
    #print str(input_file)
    delim_type = '|'
    data = csv.reader(input_file, delimiter=str(delim_type))
    data = [row for row in data]
    max_len = [max(len(str(x)) for x in line) for line in zip(*data)]
    fields = fill_zero_length(max_len)
    dupe_groups = {}
    db_dupes = {}
    for line in data:
        dupe = line[-1]
        db_dupe = line[-2]
        # count intra dupes
        if dupe in dupe_groups:
            dupe_groups[dupe] += 1
        else:
            dupe_groups[dupe] = 1
        #count db matches
        if (db_dupe != '' and db_dupe != 'CLIENT_ID'):
            if db_dupe in db_dupes:
                db_dupes[db_dupe] += 1
            else:
                db_dupes[db_dupe] = 1
    #count intra dupe groups
    groups = []
    for group in dupe_groups:
        if dupe_groups[group] > 1:
            groups.append(group)
    #count db dupe groups
    db_groups = []
    for db_group in db_dupes:
        if db_dupes[db_group] > 1:
            db_groups.append(db_group)
    print len(db_groups)
    db_count = 0
    for each in db_dupes:
        db_count += db_dupes[each]
    #write reports
    sort_data = sorted(data, key=lambda dupes: dupes[-1])
    input_recs = len(data) - 1 
    out_file.write('Input records: ' + str(input_recs) + '\n')
    out_file.write('Number of input records that match PIE: ' + str(db_count) + '\n')
    db_group_count = len(db_groups)
    out_file.write("Number of CLIENT_ID dupe groups: " + str(db_group_count) + '\n')
    dbd_count = 0
    for line in sort_data:
        dbd = line[-2]
        if dbd in db_groups:
            dbd_count += 1
    #out_file.write('Number of records that match PIE: ' + str(dbd_count) + '\n')
    space_flag = ''
    rec_count = 0
    for line in sort_data:
        dupe = line[-1]
        if dupe in groups:
            rec_count += 1
            if dupe != space_flag:
                out_file.write('\n')
            space_flag = dupe
            count = 0
            num_fields = len(line)
            new = ''
            pos = 0
            for field in line:
                try:
                    if len(field) > abs(fields[count]):
                        new = new + field[:abs(fields[count])]   #truncataes if field longer than layout (for header records)
                        count = count + 1
                    else:
                        if fields[count] < 1:
                            new = new + field.rjust(abs(fields[count]))   #right justify if all digits
                        else:
                            new = new + field.ljust(fields[count])
                        count = count + 1
                except IndexError:
                    print line
            out_file.write(new + '\n')
    group_count = len(groups)
    out_file.write('\nIntra file dupe groups: ' + str(group_count) + '\n')
    out_file.write('Total records in dupe groups: ' + str(rec_count) + '\n')
    recs_writen = (input_recs - rec_count) + group_count
    out_file.write('\nRecords writen to DB: ' + str(recs_writen))
    new_companies = recs_writen - ((db_count - dbd_count) + db_group_count)
    out_file.write('\nNew companies added to DB: ' + str(new_companies))

#def strip_suffix(name):  #strip suffix to create output file name w/'.csv'
#    return name[:-4]
#
#file_ext = '.ff'
#
#fname = 'test.Txt'
#with open(fname, 'r') as input_file, open(strip_suffix(fname) + file_ext, 'w') as out_file:
#                pie_dedupe(input_file, out_file)



