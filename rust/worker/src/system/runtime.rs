pub(crate) trait Runtime {
    fn spawn(&self, f: Box<dyn FnOnce() + Send>) -> ();
}
