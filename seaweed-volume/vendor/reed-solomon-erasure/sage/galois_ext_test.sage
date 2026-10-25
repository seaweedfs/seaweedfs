GF256.<a> = FiniteField(256)
R.<x> = GF256[x]
ext_poly = R.irreducible_element(2,algorithm="first_lexicographic" )
ExtField.<b> = GF256.extension(ext_poly)
print ExtField
print len(ExtField)

x^2 + a*x + a^7

e1 = (a^7 + a^6 + a^4 + a)*b + a^3 + a^2 + a + 1
e2 = (a^7 + a^5 + a^2)*b + a^7 + a^4 + a^3 + a

print "e1: ", e1
print "e2: ", e2

print "e1 + e2: ", e1 + e2
#(a^6 + a^5 + a^4 + a^2 + a)*b + a^7 + a^4 + a^2 + 1

print "e1 * e2: ", e1 * e2
#(a^4 + a^2 + a + 1)*b + a^7 + a^5 + a^3 + a

print "e1 / e2: ", e1 / e2
#(a^7 + a^6 + a^5 + a^4 + a^3 + a^2 + 1)*b + a^6 + a^3 + a

print "1/b: ", 1/b
#(a^4 + a^3 + a + 1)*b + a^5 + a^4 + a^2 + a