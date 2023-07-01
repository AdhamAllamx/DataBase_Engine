package DB_Engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;


class SortingOnKey implements Comparator<String[]> {
    private int index;

    public SortingOnKey(int index) {
        this.index = index;
    }

    @Override
    public int compare(String[] array1, String[] array2) {
        String element1 = array1[index];
        String element2 = array2[index];
        return element1.compareTo(element2);
    }
}

