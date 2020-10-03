use crossbeam_utils::thread;
use std::collections::HashMap;

pub trait Task: Send + Sync {
    /// given the node and the current global context (for read only)
    /// run the task however you need to, then return a tuple of a success value
    /// and an optional ContextDiff if the caller needs to modify the global context
    fn run(&self, node: &Node, global_context: &GlobalContext) ->
        (bool, Option<Vec<ContextDiff>>);
}

pub enum ContextDiff {
    CDSet(String, String),
    CDRemove(String),
}
pub use ContextDiff::*;

#[derive(Default, Clone)]
pub struct GlobalContext<'a> {
    pub known_nodes: HashMap<String, Node<'a>>,
    pub variables: HashMap<String, String>,
}
impl<'a> GlobalContext<'a> {
    fn take_diff(&mut self, diff: ContextDiff) {
        match diff {
            CDSet(skey, sval) => {
                self.variables.insert(skey, sval);
            },
            CDRemove(skey) => {
                self.variables.remove(&skey);
            }
        }
    }
}

#[derive(Clone)]
pub enum NodeType<'a> {
    NodeTypeSeries(Vec<Node<'a>>),
    NodeTypeParallel(Vec<Node<'a>>),
    NodeTypeTask,
}
impl<'a> Default for NodeType<'a> {
    fn default() -> Self {
        NodeTypeTask
    }
}
pub use NodeType::*;

#[derive(Default, Clone)]
pub struct Node<'a> {
    pub is_root_node: bool,
    pub name: Option<&'a str>,
    pub ntype: NodeType<'a>,
    pub task: Option<&'a dyn Task>,
    pub properties: HashMap<&'a str, &'a str>,
    pub continue_on_fail: bool,
}

pub fn run_node_series<'a>(
    nodes: &Vec<Node<'a>>,
    global_context: &Option<&'a GlobalContext>,
    mut_global_context: &mut Option<&'a mut GlobalContext>,
    continue_on_fail: bool,
) -> (bool, Option<Vec<ContextDiff>>) {
    let mut success = true;
    let mut diff_vec = vec![];
    for n in nodes.iter() {
        let (status, c) = run_node(n, global_context, mut_global_context);
        if !status && !continue_on_fail {
            return (false, c);
        }
        if let Some(diffs) = c {
            for diff in diffs {
                diff_vec.push(diff);
            }
        }
        success = status;
    }
    let diff_vec_opt = if diff_vec.len() > 0 { Some(diff_vec) } else { None };
    (success, diff_vec_opt)
}

pub fn run_node_parallel<'a>(
    nodes: &Vec<Node<'a>>,
    global_context: &Option<&'a GlobalContext>,
    mut_global_context: &mut Option<&'a mut GlobalContext>,
) -> (bool, Option<Vec<ContextDiff>>) {
    // TODO: figure out how to not have 2 branches here doing mostly the same thing..
    // I got stuck on rust mutability issues, so easiest solution was just branch it

    // two cases here: we either are given a mutable, or immutable global context
    // if mutable, add some logic to the thread collection for us to modify
    // the mutable global context ourselves. otherwise, in the else section
    // we just return a vec of diffs for our caller to apply
    if let Some(mgc) = mut_global_context {
        let mgc_deref = &*mgc;
        let gc: &GlobalContext = mgc_deref;
        let gc_opt = Some(gc);
        let values = thread::scope(|scope| {
            // TODO: maybe add max concurrent threads?
            let handles = (0..nodes.len()).into_iter()
                .map(|i| {
                    scope.spawn(move |_| {
                        // concurrent threads cannot modify same
                        // memory location, so we pass none explicitly
                        // however, they will all return their diffs of
                        // what they want the state to be changed to
                        // and then we apply that diff when we collect it
                        let mut mut_none = None;
                        run_node(&nodes[i], &gc_opt, &mut mut_none)
                    })
                });
            let mut diff_vec = vec![];
            let mut success = true;
            let collected = handles.collect::<Vec<_>>();
            for c in collected {
                success = match c.join() {
                    Ok(s) => {
                        match s.1 {
                            None => (),
                            Some(diff_v) => {
                                // TODO: any logic on order of this
                                // application? potentially this can be overwriting
                                // each others data...
                                for diff in diff_v {
                                    diff_vec.push(diff);
                                }
                            }
                        }
                        s.0 && s.0 == success
                    },
                    _ => false,
                };
            }
            (success, Some(diff_vec))
        });

        let (success, diff_vec) = values.unwrap();
        for diff in diff_vec.unwrap() {
            mgc.take_diff(diff);
        }
        (success, None)
    } else {
        thread::scope(|scope| {
            // TODO: maybe add max concurrent threads?
            let handles = (0..nodes.len()).into_iter()
                .map(|i| {
                    scope.spawn(move |_| {
                        let mut mut_none = None;
                        run_node(&nodes[i], global_context, &mut mut_none)
                    })
                });
            let mut diff_vec = vec![];
            let mut success = true;
            let collected = handles.collect::<Vec<_>>();
            for c in collected {
                success = match c.join() {
                    Ok(s) => {
                        match s.1 {
                            None => (),
                            Some(diff_v) => {
                                // TODO: any logic on order of this
                                // application? potentially this can be overwriting
                                // each others data...
                                for diff in diff_v {
                                    diff_vec.push(diff);
                                }
                            }
                        }
                        s.0 && s.0 == success
                    },
                    _ => false,
                };
            }
            let diff_vec_opt = if diff_vec.len() > 0 { Some(diff_vec) } else { None };
            (success, diff_vec_opt)
        }).unwrap()
    }
}

pub fn run_node_task<'a>(
    node: &Node<'a>,
    global_context: &Option<&'a GlobalContext>,
    mut_global_context: &mut Option<&'a mut GlobalContext>,
) -> (bool, Option<Vec<ContextDiff>>) {
    match node.task {
        None => (false, None),
        Some(cb) => {
            let (success, context_diff) = if let Some(mgc) = mut_global_context {
                cb.run(node, *mgc)
            } else if let Some(gc) = global_context {
                cb.run(node, *gc)
            } else {
                panic!("unsupported usage")
            };

            // if the task said there is nothing to modify,
            // then just return the success flag
            // or if there is a context to modify, but our caller
            // did not give us a mutable context to modify, then
            // return the diff, and let the caller handle applying it
            if context_diff.is_none() || mut_global_context.is_none() {
                return (success, context_diff)
            }

            // if the task provides a global context
            // diff, and our caller allows us to modify the
            // mut_global_context, then we 'apply' that
            // diff to the actual global context.
            if let Some(mgc) = mut_global_context {
                let context_diff = context_diff.unwrap();
                for diff in context_diff {
                    mgc.take_diff(diff);
                }
            }

            // we return none because we were allowed to
            // modify the global context above,
            // so no point in telling our caller to modify for us
            (success, None)
        },
    }
}

// this should really be private, but I thought maybe I might need it to be public
// at some point ¯\_(ツ)_/¯
// run_node is a recursive fn that will take a context (both as a mutable reference
// and immutable reference which point to the same thing), and iterate through
// the node higherarchy and on each leaf node (that is a task node) it will
// do node.task.run() and modify the global context if mutable, otherwise
// it will return a vector of diffs to the caller for the caller to apply
// if/as needed
pub fn run_node<'a>(
    node: &Node<'a>,
    global_context: &Option<&'a GlobalContext>,
    mut_global_context: &mut Option<&'a mut GlobalContext>,
) -> (bool, Option<Vec<ContextDiff>>)
{
    match node.ntype {
        NodeTypeSeries(ref v) => {
            run_node_series(v, global_context, mut_global_context, node.continue_on_fail)
        }
        NodeTypeParallel(ref v) => {
            run_node_parallel(v, global_context, mut_global_context)
        }
        NodeTypeTask => {
            run_node_task(node, global_context, mut_global_context)
        }
    }
}

// this is a public helper to call `run_node`. it takes a single
// mutable refernece to a global context, and calls run_node with that reference
// as both mutable, and imutable (because run_node needs access to a mutable in some cases
// and immutable in others)
pub fn run_node_helper<'a>(
    node: &Node<'a>,
    global_context: &'a mut GlobalContext,
) -> (bool, Option<Vec<ContextDiff>>)
{
    let none = None;
    let mut some = Some(global_context);
    run_node(
        node,
        &none,
        &mut some,
    )
}

#[cfg(test)]
mod test {
    use super::*;
    struct MyTask<T: Send + Sync>
        where T: Fn() -> bool,
    {
        cb: T,
    }

    impl<T: Send + Sync> MyTask<T>
        where T: Fn() -> bool,
    {
        fn do_cb(&self) -> bool {
            let cb = &self.cb;
            cb()
        }
    }

    impl<T: Send + Sync> Task for MyTask<T>
        where T: Fn() -> bool,
    {
        fn run(&self, node: &Node, global_context: &GlobalContext) ->
            (bool, Option<Vec<ContextDiff>>)
        {
            let out_diff = if let Some(s) = node.name  {
                Some(vec![CDSet(s.into(), "".into())])
            } else {
                None
            };
            (self.do_cb(), out_diff)
        }
    }

    fn make_root_node_with_series<'a>(
        series_size: usize,
        task: &'a dyn Task,
        name_vec: &'a mut Vec<&str>,
    ) -> Node<'a> {
        let mut root = Node::default();
        let mut node_vec = vec![];
        for i in 0..series_size {
            let mut task_node = Node::default();
            task_node.ntype = NodeTypeTask;
            if name_vec.len() > i {
                task_node.name = Some(name_vec[i]);
            }
            task_node.task = Some(task);
            node_vec.push(task_node);
        }
        root.ntype = NodeTypeSeries(node_vec);
        root
    }

    #[test]
    fn returns_true_if_task_successful() {
        let mut mycontext = GlobalContext::default();
        let mytask = MyTask { cb: || true };
        let mut root = Node::default();
        root.ntype = NodeTypeTask;
        root.task = Some(&mytask);
        let (result, _) = run_node_helper(&root, &mut mycontext);
        assert_eq!(result, true);
    }

    #[test]
    fn returns_false_if_task_fails() {
        let mut mycontext = GlobalContext::default();
        let mytask = MyTask { cb: || false };
        let mut root = Node::default();
        root.ntype = NodeTypeTask;
        root.task = Some(&mytask);
        let (result, _) = run_node_helper(&root, &mut mycontext);
        assert_eq!(result, false);
    }

    #[test]
    fn returns_true_if_all_tasks_successful_in_series() {
        let mut mycontext = GlobalContext::default();
        let mytask = MyTask { cb: || true };
        let mut strvec = vec![];
        let root = make_root_node_with_series(3, &mytask, &mut strvec);
        let (result, _) = run_node_helper(&root, &mut mycontext);
        assert_eq!(result, true);
    }

    #[test]
    fn returns_false_if_one_tasks_fails_in_series() {
        let mut mycontext = GlobalContext::default();
        let mytask = MyTask { cb: || true };
        let mut strvec = vec!["a", "b", "c", "d", "e"];
        let mut root = make_root_node_with_series(5, &mytask, &mut strvec);

        // the third task should fail, so the global
        // context should not have d because d never gets ran
        let myfailtask = MyTask { cb: || false };
        if let NodeTypeSeries(ref mut s) = root.ntype {
            s[2].task = Some(&myfailtask);
        }
        let (result, _) = run_node_helper(&root, &mut mycontext);
        assert_eq!(result, false);

        // currently this sets the global context to have 'c',
        // but should it? if it failed on 'c', should it be allowed
        // to modify context?
        assert!(mycontext.variables.contains_key("a"));
        assert!(mycontext.variables.contains_key("b"));
    }
}
