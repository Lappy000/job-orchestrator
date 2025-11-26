"""
State machine implementation for the Job Orchestrator.

This module provides the StateMachine class for managing job lifecycle
state transitions with validation and hook support.
"""

from collections import defaultdict
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Protocol, Set
import threading
import logging

from .job import Job, JobState
from .exceptions import InvalidStateTransitionError


if TYPE_CHECKING:
    from ..storage.base import BaseStorage


logger = logging.getLogger(__name__)


class JobStore(Protocol):
    """Protocol for job storage backends used by StateMachine."""
    
    def update(self, job: Job) -> None:
        """Update a job in the store."""
        ...


# Type alias for hook callbacks
StateHook = Callable[[Job], None]
TransitionHook = Callable[[Job, JobState, JobState], None]


class StateMachine:
    """
    Job state machine with validation and hooks.
    
    Ensures only valid state transitions occur and provides hooks
    for state change notifications. This class is thread-safe and
    can be used from multiple workers simultaneously.
    
    Attributes:
        VALID_TRANSITIONS: Mapping of valid state transitions.
        TERMINAL_STATES: Set of states that represent job completion.
        ACTIVE_STATES: Set of states that represent active jobs.
    
    Example:
        >>> sm = StateMachine()
        >>> job = Job(name="test")
        >>> sm.transition(job, JobState.SCHEDULED)
        >>> sm.transition(job, JobState.RUNNING)
        >>> job.state
        <JobState.RUNNING: 'running'>
    """
    
    # Define valid state transitions
    VALID_TRANSITIONS: Dict[JobState, Set[JobState]] = {
        JobState.PENDING: {JobState.SCHEDULED, JobState.CANCELLED},
        JobState.SCHEDULED: {JobState.RUNNING, JobState.CANCELLED},
        JobState.RUNNING: {
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.RETRYING,
            JobState.TIMEOUT,
        },
        JobState.RETRYING: {JobState.SCHEDULED, JobState.CANCELLED, JobState.FAILED},
        JobState.TIMEOUT: {JobState.RETRYING, JobState.FAILED},
        JobState.COMPLETED: set(),  # Terminal state
        JobState.FAILED: set(),     # Terminal state
        JobState.CANCELLED: set(),  # Terminal state
    }
    
    # Terminal states (job finished)
    TERMINAL_STATES: Set[JobState] = {
        JobState.COMPLETED,
        JobState.FAILED,
        JobState.CANCELLED,
    }
    
    # Active states (job in progress)
    ACTIVE_STATES: Set[JobState] = {
        JobState.PENDING,
        JobState.SCHEDULED,
        JobState.RUNNING,
        JobState.RETRYING,
    }
    
    def __init__(self):
        """Initialize the state machine."""
        self._lock = threading.RLock()
        self._state_hooks: Dict[JobState, List[StateHook]] = defaultdict(list)
        self._transition_hooks: Dict[tuple, List[TransitionHook]] = defaultdict(list)
        self._global_hooks: List[TransitionHook] = []
        self._job_store: Optional[JobStore] = None
    
    def set_job_store(self, store: JobStore) -> None:
        """
        Set the job store for persistence.
        
        When a job store is set, job state changes will be automatically
        persisted after each transition.
        
        Args:
            store: A storage backend implementing the JobStore protocol.
        """
        self._job_store = store
    
    def can_transition(self, current: JobState, target: JobState) -> bool:
        """
        Check if transition from current to target state is valid.
        
        Args:
            current: The current state of the job.
            target: The desired target state.
            
        Returns:
            True if the transition is valid, False otherwise.
        """
        return target in self.VALID_TRANSITIONS.get(current, set())
    
    def get_valid_transitions(self, current: JobState) -> Set[JobState]:
        """
        Get all valid transitions from the current state.
        
        Args:
            current: The current state of the job.
            
        Returns:
            Set of states that can be transitioned to.
        """
        return self.VALID_TRANSITIONS.get(current, set()).copy()
    
    def transition(
        self,
        job: Job,
        target: JobState,
        *,
        persist: bool = True,
        execute_hooks: bool = True
    ) -> None:
        """
        Transition a job to a new state.
        
        Validates the transition, updates the job state, persists the
        change (if a job store is configured), and executes any registered
        hooks.
        
        Args:
            job: The job to transition.
            target: The target state.
            persist: If True and a job store is set, persist the change.
            execute_hooks: If True, execute registered hooks.
            
        Raises:
            InvalidStateTransitionError: If the transition is not valid.
        """
        with self._lock:
            current = job.state
            
            if not self.can_transition(current, target):
                raise InvalidStateTransitionError(
                    job_id=job.id,
                    current_state=current.value,
                    target_state=target.value,
                )
            
            # Update state
            old_state = job.state
            job.state = target
            
            logger.debug(
                f"Job {job.id} transitioned: {old_state.value} -> {target.value}"
            )
            
            # Persist state change
            if persist and self._job_store:
                try:
                    self._job_store.update(job)
                except Exception as e:
                    # Rollback state on persistence failure
                    job.state = old_state
                    logger.error(f"Failed to persist job {job.id} state: {e}")
                    raise
            
            # Execute hooks
            if execute_hooks:
                self._execute_hooks(job, old_state, target)
    
    def register_state_hook(
        self,
        state: JobState,
        callback: StateHook
    ) -> None:
        """
        Register a callback for when a job enters a state.
        
        The callback will be called with the job as the only argument
        whenever any job enters the specified state.
        
        Args:
            state: The state to watch for.
            callback: Function to call when a job enters the state.
        """
        with self._lock:
            self._state_hooks[state].append(callback)
    
    def unregister_state_hook(
        self,
        state: JobState,
        callback: StateHook
    ) -> bool:
        """
        Unregister a state hook callback.
        
        Args:
            state: The state the callback was registered for.
            callback: The callback to remove.
            
        Returns:
            True if the callback was found and removed, False otherwise.
        """
        with self._lock:
            try:
                self._state_hooks[state].remove(callback)
                return True
            except ValueError:
                return False
    
    def register_transition_hook(
        self,
        from_state: JobState,
        to_state: JobState,
        callback: TransitionHook
    ) -> None:
        """
        Register a callback for a specific state transition.
        
        The callback will be called with (job, from_state, to_state)
        whenever a job transitions between the specified states.
        
        Args:
            from_state: The source state of the transition.
            to_state: The target state of the transition.
            callback: Function to call when the transition occurs.
        """
        with self._lock:
            self._transition_hooks[(from_state, to_state)].append(callback)
    
    def unregister_transition_hook(
        self,
        from_state: JobState,
        to_state: JobState,
        callback: TransitionHook
    ) -> bool:
        """
        Unregister a transition hook callback.
        
        Args:
            from_state: The source state of the transition.
            to_state: The target state of the transition.
            callback: The callback to remove.
            
        Returns:
            True if the callback was found and removed, False otherwise.
        """
        with self._lock:
            key = (from_state, to_state)
            try:
                self._transition_hooks[key].remove(callback)
                return True
            except ValueError:
                return False
    
    def register_global_hook(self, callback: TransitionHook) -> None:
        """
        Register a callback for all state transitions.
        
        The callback will be called with (job, from_state, to_state)
        for every state transition.
        
        Args:
            callback: Function to call on every transition.
        """
        with self._lock:
            self._global_hooks.append(callback)
    
    def unregister_global_hook(self, callback: TransitionHook) -> bool:
        """
        Unregister a global hook callback.
        
        Args:
            callback: The callback to remove.
            
        Returns:
            True if the callback was found and removed, False otherwise.
        """
        with self._lock:
            try:
                self._global_hooks.remove(callback)
                return True
            except ValueError:
                return False
    
    def _execute_hooks(
        self,
        job: Job,
        old_state: JobState,
        new_state: JobState
    ) -> None:
        """
        Execute registered hooks for state change.
        
        Hooks are executed in the following order:
        1. Global hooks (all transitions)
        2. Transition-specific hooks
        3. State-specific hooks (for the new state)
        
        Exceptions from hooks are logged but don't prevent other hooks
        from executing or the state transition from completing.
        
        Args:
            job: The job that transitioned.
            old_state: The previous state.
            new_state: The new state.
        """
        # Global hooks
        for callback in self._global_hooks:
            try:
                callback(job, old_state, new_state)
            except Exception as e:
                logger.error(
                    f"Global hook error for job {job.id} "
                    f"({old_state.value} -> {new_state.value}): {e}"
                )
        
        # Transition-specific hooks
        for callback in self._transition_hooks.get((old_state, new_state), []):
            try:
                callback(job, old_state, new_state)
            except Exception as e:
                logger.error(
                    f"Transition hook error for job {job.id} "
                    f"({old_state.value} -> {new_state.value}): {e}"
                )
        
        # State-specific hooks
        for callback in self._state_hooks[new_state]:
            try:
                callback(job)
            except Exception as e:
                logger.error(
                    f"State hook error for job {job.id} "
                    f"entering {new_state.value}: {e}"
                )
    
    @staticmethod
    def is_terminal(state: JobState) -> bool:
        """
        Check if a state is terminal (job finished).
        
        Args:
            state: The state to check.
            
        Returns:
            True if the state is terminal, False otherwise.
        """
        return state in StateMachine.TERMINAL_STATES
    
    @staticmethod
    def is_active(state: JobState) -> bool:
        """
        Check if a job is currently active (not finished).
        
        Args:
            state: The state to check.
            
        Returns:
            True if the state is active, False otherwise.
        """
        return state in StateMachine.ACTIVE_STATES
    
    def clear_hooks(self) -> None:
        """Remove all registered hooks."""
        with self._lock:
            self._state_hooks.clear()
            self._transition_hooks.clear()
            self._global_hooks.clear()
    
    def get_transition_graph(self) -> Dict[str, List[str]]:
        """
        Get the state transition graph as a dictionary.
        
        Useful for visualization or debugging.
        
        Returns:
            Dictionary mapping state names to lists of valid target states.
        """
        return {
            state.value: [s.value for s in targets]
            for state, targets in self.VALID_TRANSITIONS.items()
        }


__all__ = [
    "StateMachine",
    "StateHook",
    "TransitionHook",
    "JobStore",
]