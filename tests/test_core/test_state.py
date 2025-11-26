"""
Tests for StateMachine class.

This module tests the state machine implementation, including state transitions,
validation, hooks, and all state-related functionality.
"""

import pytest
from unittest.mock import Mock, call

from job_orchestrator import JobState
from job_orchestrator.core.state import StateMachine, StateHook, TransitionHook
from job_orchestrator.core.exceptions import InvalidStateTransitionError


class TestStateMachineCreation:
    """Tests for StateMachine creation and initialization."""

    def test_state_machine_creation_default(self, state_machine):
        """Test creating state machine with default configurations."""
        assert state_machine is not None
        assert isinstance(state_machine, StateMachine)

    def test_state_machine_has_valid_transitions(self, state_machine):
        """Test that state machine defines valid transitions."""
        assert hasattr(state_machine, 'VALID_TRANSITIONS')
        assert JobState.PENDING in state_machine.VALID_TRANSITIONS

    def test_state_machine_transitions_dict(self, state_machine):
        """Test all states have transitions defined."""
        expected_states = {
            JobState.PENDING,
            JobState.SCHEDULED,
            JobState.RUNNING,
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.RETRYING,
            JobState.CANCELLED,
            JobState.TIMEOUT,
        }
        
        assert set(state_machine.VALID_TRANSITIONS.keys()) == expected_states

    def test_state_machine_creation(self):
        """Test creating state machine."""
        sm = StateMachine()
        
        assert sm is not None
        assert isinstance(sm, StateMachine)


class TestValidTransitions:
    """Tests for all valid state transitions."""

    def test_pending_to_scheduled(self, state_machine, sample_job):
        """Test transition from PENDING to SCHEDULED."""
        assert state_machine.can_transition(JobState.PENDING, JobState.SCHEDULED)
        
        state_machine.transition(sample_job, JobState.SCHEDULED)
        
        assert sample_job.state == JobState.SCHEDULED

    def test_pending_to_cancelled(self, state_machine, sample_job):
        """Test transition from PENDING to CANCELLED."""
        assert state_machine.can_transition(JobState.PENDING, JobState.CANCELLED)
        
        state_machine.transition(sample_job, JobState.CANCELLED)
        
        assert sample_job.state == JobState.CANCELLED

    def test_scheduled_to_running(self, state_machine):
        """Test transition from SCHEDULED to RUNNING."""
        assert state_machine.can_transition(JobState.SCHEDULED, JobState.RUNNING)

    def test_scheduled_to_cancelled(self, state_machine):
        """Test transition from SCHEDULED to CANCELLED."""
        assert state_machine.can_transition(JobState.SCHEDULED, JobState.CANCELLED)

    def test_running_to_completed(self, state_machine):
        """Test transition from RUNNING to COMPLETED."""
        assert state_machine.can_transition(JobState.RUNNING, JobState.COMPLETED)

    def test_running_to_failed(self, state_machine):
        """Test transition from RUNNING to FAILED."""
        assert state_machine.can_transition(JobState.RUNNING, JobState.FAILED)

    def test_running_to_retrying(self, state_machine):
        """Test transition from RUNNING to RETRYING."""
        assert state_machine.can_transition(JobState.RUNNING, JobState.RETRYING)

    def test_running_to_timeout(self, state_machine):
        """Test transition from RUNNING to TIMEOUT."""
        assert state_machine.can_transition(JobState.RUNNING, JobState.TIMEOUT)

    def test_retrying_to_scheduled(self, state_machine):
        """Test transition from RETRYING to SCHEDULED."""
        assert state_machine.can_transition(JobState.RETRYING, JobState.SCHEDULED)

    def test_timeout_to_retrying(self, state_machine):
        """Test transition from TIMEOUT to RETRYING."""
        assert state_machine.can_transition(JobState.TIMEOUT, JobState.RETRYING)


class TestInvalidTransitions:
    """Tests for invalid state transitions that should raise exceptions."""

    def test_completed_to_any_raises(self, state_machine):
        """Test that transitions from COMPLETED raise exception."""
        from job_orchestrator import Job
        
        # COMPLETED is a terminal state
        job1 = Job(name="test_completed")
        job1.state = JobState.COMPLETED
        
        with pytest.raises(InvalidStateTransitionError):
            state_machine.transition(job1, JobState.PENDING)
        
        job2 = Job(name="test_completed2")
        job2.state = JobState.COMPLETED
        
        with pytest.raises(InvalidStateTransitionError):
            state_machine.transition(job2, JobState.RUNNING)

    def test_cancelled_to_any_raises(self, state_machine):
        """Test that transitions from CANCELLED raise exception."""
        from job_orchestrator import Job
        
        # CANCELLED is a terminal state
        job = Job(name="test_cancelled")
        job.state = JobState.CANCELLED
        
        with pytest.raises(InvalidStateTransitionError):
            state_machine.transition(job, JobState.PENDING)

    def test_pending_to_completed_raises(self, state_machine, sample_job):
        """Test direct transition from PENDING to COMPLETED raises."""
        with pytest.raises(InvalidStateTransitionError):
            state_machine.transition(sample_job, JobState.COMPLETED)

    def test_pending_to_failed_raises(self, state_machine, sample_job):
        """Test direct transition from PENDING to FAILED raises."""
        with pytest.raises(InvalidStateTransitionError):
            state_machine.transition(sample_job, JobState.FAILED)

    def test_running_to_scheduled_raises(self, state_machine):
        """Test backward transition from RUNNING to SCHEDULED raises."""
        from job_orchestrator import Job
        
        job = Job(name="test_running")
        job.state = JobState.RUNNING
        
        with pytest.raises(InvalidStateTransitionError):
            state_machine.transition(job, JobState.SCHEDULED)

    def test_running_to_pending_raises(self, state_machine):
        """Test backward transition from RUNNING to PENDING raises."""
        from job_orchestrator import Job
        
        job = Job(name="test_running")
        job.state = JobState.RUNNING
        
        with pytest.raises(InvalidStateTransitionError):
            state_machine.transition(job, JobState.PENDING)

    def test_failed_to_completed_raises(self, state_machine):
        """Test transition from FAILED to COMPLETED raises."""
        from job_orchestrator import Job
        
        job = Job(name="test_failed")
        job.state = JobState.FAILED
        
        with pytest.raises(InvalidStateTransitionError):
            state_machine.transition(job, JobState.COMPLETED)


class TestCanTransition:
    """Tests for can_transition method."""

    def test_can_transition_valid(self, state_machine):
        """Test can_transition returns True for valid transitions."""
        # PENDING can only transition to SCHEDULED or CANCELLED
        assert state_machine.can_transition(JobState.PENDING, JobState.SCHEDULED) is True

    def test_can_transition_invalid(self, state_machine):
        """Test can_transition returns False for invalid transitions."""
        assert state_machine.can_transition(JobState.COMPLETED, JobState.PENDING) is False

    def test_can_transition_same_state(self, state_machine):
        """Test can_transition for same state (should be invalid)."""
        assert state_machine.can_transition(JobState.PENDING, JobState.PENDING) is False


class TestGetAllowedTransitions:
    """Tests for getting allowed transitions from a state."""

    def test_get_valid_transitions_from_pending(self, state_machine):
        """Test getting valid transitions from PENDING."""
        allowed = state_machine.get_valid_transitions(JobState.PENDING)
        
        assert JobState.SCHEDULED in allowed
        assert JobState.CANCELLED in allowed
        assert JobState.COMPLETED not in allowed

    def test_get_valid_transitions_from_running(self, state_machine):
        """Test getting valid transitions from RUNNING."""
        allowed = state_machine.get_valid_transitions(JobState.RUNNING)
        
        assert JobState.COMPLETED in allowed
        assert JobState.FAILED in allowed
        assert JobState.RETRYING in allowed
        assert JobState.TIMEOUT in allowed

    def test_get_valid_transitions_from_terminal_state(self, state_machine):
        """Test getting valid transitions from terminal state."""
        completed_transitions = state_machine.get_valid_transitions(JobState.COMPLETED)
        cancelled_transitions = state_machine.get_valid_transitions(JobState.CANCELLED)
        
        assert len(completed_transitions) == 0
        assert len(cancelled_transitions) == 0


class TestStateHooks:
    """Tests for state transition hooks."""

    def test_state_hook_is_called(self, state_machine, sample_job):
        """Test state hook is called when entering a state."""
        mock_hook = Mock()
        state_machine.register_state_hook(JobState.SCHEDULED, mock_hook)
        
        state_machine.transition(sample_job, JobState.SCHEDULED)
        
        mock_hook.assert_called_once_with(sample_job)

    def test_transition_hook(self, state_machine, sample_job):
        """Test transition hook is called for specific transition."""
        mock_hook = Mock()
        state_machine.register_transition_hook(JobState.PENDING, JobState.SCHEDULED, mock_hook)
        
        state_machine.transition(sample_job, JobState.SCHEDULED)
        
        mock_hook.assert_called_once()

    def test_transition_hook_not_called_for_other(self, state_machine, sample_job):
        """Test transition hook not called for other transitions."""
        mock_hook = Mock()
        state_machine.register_transition_hook(JobState.PENDING, JobState.SCHEDULED, mock_hook)
        
        state_machine.transition(sample_job, JobState.CANCELLED)
        
        mock_hook.assert_not_called()

    def test_global_hook_called(self, state_machine, sample_job):
        """Test global hook is called for all transitions."""
        call_count = [0]
        
        def global_hook(job, from_state, to_state):
            call_count[0] += 1
        
        state_machine.register_global_hook(global_hook)
        
        state_machine.transition(sample_job, JobState.SCHEDULED)
        
        assert call_count[0] == 1


class TestStateMachineGraph:
    """Tests for state machine visualization."""

    def test_get_transition_graph(self, state_machine):
        """Test getting the transition graph."""
        graph = state_machine.get_transition_graph()
        
        assert isinstance(graph, dict)
        assert "pending" in graph
        assert "running" in graph
        assert "completed" in graph

    def test_transition_graph_values(self, state_machine):
        """Test transition graph values are correct."""
        graph = state_machine.get_transition_graph()
        
        # PENDING can go to SCHEDULED and CANCELLED
        assert "scheduled" in graph["pending"]
        assert "cancelled" in graph["pending"]
        
        # COMPLETED has no outgoing transitions
        assert len(graph["completed"]) == 0


class TestTerminalStates:
    """Tests for terminal state detection."""

    def test_is_terminal_completed(self, state_machine):
        """Test COMPLETED is a terminal state."""
        assert StateMachine.is_terminal(JobState.COMPLETED) is True

    def test_is_terminal_cancelled(self, state_machine):
        """Test CANCELLED is a terminal state."""
        assert StateMachine.is_terminal(JobState.CANCELLED) is True

    def test_is_terminal_pending(self, state_machine):
        """Test PENDING is not a terminal state."""
        assert StateMachine.is_terminal(JobState.PENDING) is False

    def test_is_terminal_running(self, state_machine):
        """Test RUNNING is not a terminal state."""
        assert StateMachine.is_terminal(JobState.RUNNING) is False

    def test_is_terminal_failed(self, state_machine):
        """Test FAILED is a terminal state."""
        assert StateMachine.is_terminal(JobState.FAILED) is True

    def test_terminal_states_constant(self, state_machine):
        """Test TERMINAL_STATES constant."""
        terminal = StateMachine.TERMINAL_STATES
        
        assert JobState.COMPLETED in terminal
        assert JobState.CANCELLED in terminal
        assert JobState.FAILED in terminal
        assert JobState.PENDING not in terminal


class TestActiveStates:
    """Tests for active state detection."""

    def test_is_active_running(self, state_machine):
        """Test RUNNING is an active state."""
        assert StateMachine.is_active(JobState.RUNNING) is True

    def test_is_active_retrying(self, state_machine):
        """Test RETRYING is an active state."""
        assert StateMachine.is_active(JobState.RETRYING) is True

    def test_is_active_pending(self, state_machine):
        """Test PENDING is an active state."""
        assert StateMachine.is_active(JobState.PENDING) is True

    def test_is_active_completed(self, state_machine):
        """Test COMPLETED is not an active state."""
        assert StateMachine.is_active(JobState.COMPLETED) is False


class TestStateMachineRepr:
    """Tests for string representation."""

    def test_state_machine_repr(self, state_machine):
        """Test repr of StateMachine."""
        result = repr(state_machine)
        
        assert "StateMachine" in result


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_clear_hooks(self, state_machine):
        """Test clearing all hooks."""
        mock_hook = Mock()
        state_machine.register_state_hook(JobState.SCHEDULED, mock_hook)
        
        state_machine.clear_hooks()
        
        # Create a new job to test
        from job_orchestrator import Job
        job = Job(name="test")
        state_machine.transition(job, JobState.SCHEDULED)
        
        mock_hook.assert_not_called()