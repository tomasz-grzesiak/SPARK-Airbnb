import re

def parse(file):
    review = ''
    listing_id = ''
    first_run = True
            
    while line := file.readline():
        if re.match(r'^Review\s\d+', line):
            if not first_run:
                review = '"' + review.replace('"', '').replace('\n', ' ').strip() + '"'   

                result.write(f'{listing_id},{review}\n')
            first_run = False

            try:
                listing_id = line.split(' ')[4]
            except: 
                print(line)
            review = ''.join(line.split(':')[1:])
        else:
            review += line

with open('result.csv', 'w', encoding="ISO-8859-1") as result:
    result.write('listing_id,review\n')

    with open('BerlinReviews.txt', 'r', encoding="ISO-8859-1") as berlin:
        parse(berlin)
    with open('MadridReviews.txt', 'r', encoding="ISO-8859-1") as madrid:
        parse(madrid)
    with open('ParisReviews.txt', 'r', encoding="ISO-8859-1") as paris:
        parse(paris)
            