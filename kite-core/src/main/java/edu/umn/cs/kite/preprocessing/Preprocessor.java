package edu.umn.cs.kite.preprocessing;

import edu.umn.cs.kite.util.IdGenerator;

import java.util.List;

/**
 * Created by amr_000 on 8/8/2016.
 */
public interface Preprocessor<S,T> {
    public T preprocess(S object, IdGenerator id);
    public List<T> preprocess(List<S> objects, IdGenerator id);
}
