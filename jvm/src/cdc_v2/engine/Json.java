package cdc_v2.engine;

import java.util.LinkedHashMap;
import java.util.Map;

/** The little JSON this program needs: a flat object of string values (CONFIG), and quoting. */
final class Json {

    static String quote(String s) {
        StringBuilder b = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    b.append("\\\"");
                    break;
                case '\\':
                    b.append("\\\\");
                    break;
                case '\n':
                    b.append("\\n");
                    break;
                case '\r':
                    b.append("\\r");
                    break;
                case '\t':
                    b.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        b.append(String.format("\\u%04x", (int) c));
                    }
                    else {
                        b.append(c);
                    }
            }
        }
        return b.append('"').toString();
    }

    /** Parses {"k": "v", ...}; every value must be a JSON string. Throws IllegalArgumentException otherwise. */
    static Map<String, String> parseStringObject(String text) {
        Parser p = new Parser(text);
        Map<String, String> out = new LinkedHashMap<>();
        p.ws();
        p.expect('{');
        p.ws();
        if (p.peek() == '}') {
            p.i++;
        }
        else {
            while (true) {
                p.ws();
                String k = p.string();
                p.ws();
                p.expect(':');
                p.ws();
                String v = p.string();
                if (out.put(k, v) != null) {
                    throw new IllegalArgumentException("duplicate key in CONFIG");
                }
                p.ws();
                char c = p.next();
                if (c == '}') {
                    break;
                }
                if (c != ',') {
                    throw new IllegalArgumentException("expected , or } in CONFIG at " + (p.i - 1));
                }
            }
        }
        p.ws();
        if (p.i != text.length()) {
            throw new IllegalArgumentException("trailing characters in CONFIG");
        }
        return out;
    }

    private static final class Parser {
        final String s;
        int i;

        Parser(String s) {
            this.s = s;
        }

        char peek() {
            if (i >= s.length()) {
                throw new IllegalArgumentException("unexpected end of CONFIG");
            }
            return s.charAt(i);
        }

        char next() {
            char c = peek();
            i++;
            return c;
        }

        void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }

        void expect(char c) {
            if (next() != c) {
                throw new IllegalArgumentException("expected " + c + " in CONFIG at " + (i - 1));
            }
        }

        String string() {
            expect('"');
            StringBuilder b = new StringBuilder();
            while (true) {
                char c = next();
                if (c == '"') {
                    return b.toString();
                }
                if (c == '\\') {
                    char e = next();
                    switch (e) {
                        case '"':
                        case '\\':
                        case '/':
                            b.append(e);
                            break;
                        case 'b':
                            b.append('\b');
                            break;
                        case 'f':
                            b.append('\f');
                            break;
                        case 'n':
                            b.append('\n');
                            break;
                        case 'r':
                            b.append('\r');
                            break;
                        case 't':
                            b.append('\t');
                            break;
                        case 'u':
                            if (i + 4 > s.length()) {
                                throw new IllegalArgumentException("bad escape in CONFIG");
                            }
                            b.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                            i += 4;
                            break;
                        default:
                            throw new IllegalArgumentException("bad escape in CONFIG");
                    }
                }
                else {
                    b.append(c);
                }
            }
        }
    }

    private Json() {
    }
}
